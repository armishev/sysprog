#include "parser.h"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <string>
#include <sys/wait.h>
#include <unistd.h>
#include <vector>

static void
reap_zombies(void)
{
	int wait_status;
	while (waitpid(-1, &wait_status, WNOHANG) > 0) {
	}
}

static bool
extract_pipeline(const struct command_line *line,
		 std::vector<command> &pipeline_commands)
{
	pipeline_commands.clear();
	bool next_segment_must_be_command = true;
	for (const expr &e : line->exprs) {
		if (next_segment_must_be_command) {
			if (e.type != EXPR_TYPE_COMMAND)
				return false;
			pipeline_commands.push_back(*e.cmd);
			next_segment_must_be_command = false;
		} else {
			if (e.type != EXPR_TYPE_PIPE)
				return false;
			next_segment_must_be_command = true;
		}
	}
	return !next_segment_must_be_command && !pipeline_commands.empty();
}

static int
parse_exit_code(const command &cmd)
{
	if (cmd.args.empty())
		return 0;
	int code_byte = std::atoi(cmd.args[0].c_str());
	return code_byte & 255;
}

static int
builtin_cd(const command &cmd)
{
	if (cmd.args.size() > 1)
		return 1;
	const char *path;
	if (cmd.args.empty()) {
		path = std::getenv("HOME");
		if (path == NULL)
			return 1;
	} else {
		path = cmd.args[0].c_str();
	}
	return chdir(path) == 0 ? 0 : 1;
}

static void
builtin_exit_child(const command &cmd)
{
	_exit(parse_exit_code(cmd));
}

static void
try_exec_external(const command &cmd)
{
	std::vector<std::string> argv_storage;
	argv_storage.reserve(1 + cmd.args.size());
	argv_storage.push_back(cmd.exe);
	for (const std::string &a : cmd.args)
		argv_storage.push_back(a);
	std::vector<char *> argv_cstrs;
	for (std::string &s : argv_storage)
		argv_cstrs.push_back(s.data());
	argv_cstrs.push_back(NULL);
	execvp(argv_cstrs[0], argv_cstrs.data());
	_exit(127);
}

static int
open_redirect(const struct command_line *line)
{
	if (line->out_type == OUTPUT_TYPE_FILE_NEW) {
		int file_fd = open(line->out_file.c_str(),
				   O_WRONLY | O_CREAT | O_TRUNC, 0644);
		return file_fd;
	}
	if (line->out_type == OUTPUT_TYPE_FILE_APPEND) {
		int file_fd = open(line->out_file.c_str(),
				   O_WRONLY | O_CREAT | O_APPEND, 0644);
		return file_fd;
	}
	return -1;
}

static int
run_pipeline(const struct command_line *line, bool job_is_background,
	     bool *shell_should_exit, int *shell_exit_code)
{
	std::vector<command> pipeline_commands;
	if (!extract_pipeline(line, pipeline_commands))
		return 1;

	const size_t num_commands = pipeline_commands.size();

	if (num_commands == 1 && pipeline_commands[0].exe == "cd" &&
	    !job_is_background)
		return builtin_cd(pipeline_commands[0]);

	if (num_commands == 1 && pipeline_commands[0].exe == "exit") {
		if (!job_is_background &&
		    line->out_type == OUTPUT_TYPE_STDOUT) {
			int code = parse_exit_code(pipeline_commands[0]);
			*shell_should_exit = true;
			*shell_exit_code = code;
			return code;
		}
	}

	std::vector<pid_t> child_pids;
	child_pids.reserve(num_commands);
	int prev_segment_pipe_read = -1;

	for (size_t segment_index = 0; segment_index < num_commands;
	     segment_index++) {
		int next_segment_pipe_write = -1;
		int next_segment_pipe_read = -1;
		if (segment_index + 1 < num_commands) {
			int link_pipe[2];
			if (pipe(link_pipe) < 0) {
				if (prev_segment_pipe_read >= 0)
					close(prev_segment_pipe_read);
				for (pid_t child_pid : child_pids) {
					int wait_status = 0;
					(void)waitpid(child_pid, &wait_status, 0);
				}
				return 1;
			}
			next_segment_pipe_read = link_pipe[0];
			next_segment_pipe_write = link_pipe[1];
		}

		pid_t child_pid = fork();
		if (child_pid < 0) {
			if (next_segment_pipe_read >= 0) {
				close(next_segment_pipe_read);
				close(next_segment_pipe_write);
			}
			if (prev_segment_pipe_read >= 0)
				close(prev_segment_pipe_read);
			for (pid_t waited_pid : child_pids) {
				int wait_status = 0;
				(void)waitpid(waited_pid, &wait_status, 0);
			}
			return 1;
		}

		if (child_pid == 0) {
			if (prev_segment_pipe_read >= 0) {
				dup2(prev_segment_pipe_read, STDIN_FILENO);
				close(prev_segment_pipe_read);
			}
			if (next_segment_pipe_write >= 0) {
				close(next_segment_pipe_read);
				dup2(next_segment_pipe_write, STDOUT_FILENO);
				close(next_segment_pipe_write);
			} else {
				int redirect_fd = -1;
				if (line->out_type != OUTPUT_TYPE_STDOUT) {
					redirect_fd = open_redirect(line);
					if (redirect_fd < 0)
						_exit(1);
					dup2(redirect_fd, STDOUT_FILENO);
					close(redirect_fd);
				}
			}

			const command &segment_cmd = pipeline_commands[segment_index];
			if (segment_cmd.exe == "exit")
				builtin_exit_child(segment_cmd);
			if (segment_cmd.exe == "cd")
				_exit(builtin_cd(segment_cmd));
			try_exec_external(segment_cmd);
		}

		if (prev_segment_pipe_read >= 0)
			close(prev_segment_pipe_read);
		if (next_segment_pipe_write >= 0)
			close(next_segment_pipe_write);
		prev_segment_pipe_read = next_segment_pipe_read;
		child_pids.push_back(child_pid);
	}

	if (prev_segment_pipe_read >= 0)
		close(prev_segment_pipe_read);

	int last_exit_status = 0;
	if (!job_is_background) {
		for (pid_t waited_pid : child_pids) {
			int wait_status = 0;
			waitpid(waited_pid, &wait_status, 0);
			if (WIFEXITED(wait_status))
				last_exit_status = WEXITSTATUS(wait_status);
			else if (WIFSIGNALED(wait_status))
				last_exit_status = 128 + WTERMSIG(wait_status);
		}
	} else {
		last_exit_status = 0;
	}

	return last_exit_status;
}

static void
execute_command_line(struct command_line *line, int *last_status,
		     bool *shell_should_exit, int *shell_exit_code)
{
	reap_zombies();

	if (line->exprs.empty()) {
		*last_status = 0;
		return;
	}

	bool job_is_background = line->is_background;
	int pipeline_status =
	    run_pipeline(line, job_is_background, shell_should_exit,
			 shell_exit_code);
	if (!*shell_should_exit)
		*last_status = pipeline_status;
}

int
main(void)
{
	struct parser *p = parser_new();
	int last_status = 0;
	const size_t buf_size = 4096;
	char buf[buf_size];
	int rc;
	bool shell_should_exit = false;
	int shell_exit_code = 0;

	while ((rc = read(STDIN_FILENO, buf, buf_size)) > 0) {
		parser_feed(p, buf, static_cast<uint32_t>(rc));
		struct command_line *line = NULL;
		while (true) {
			enum parser_error err = parser_pop_next(p, &line);
			if (err == PARSER_ERR_NONE && line == NULL)
				break;
			if (err != PARSER_ERR_NONE) {
				printf("Error: %d\n", (int)err);
				continue;
			}
			execute_command_line(line, &last_status, &shell_should_exit,
					     &shell_exit_code);
			delete line;
			if (shell_should_exit) {
				parser_delete(p);
				return shell_exit_code;
			}
		}
	}

	reap_zombies();
	parser_delete(p);
	return last_status;
}
