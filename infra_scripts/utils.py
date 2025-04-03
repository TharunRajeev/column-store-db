import os
import re
import argparse
import pandas as pd
import sys
import time


def parse_runtime_data(filepath):
    """Parse a single test output file and extract query runtime information."""
    data = []

    with open(filepath, "r") as f:
        content = f.read()

        # Find all query patterns
        query_patterns = re.finditer(
            r"--Query: ([^\n]+)\n--\s*t = ([0-9.]+)μs", content
        )

        for match in query_patterns:
            operation = match.group(1).split("(")[0]  # Extract operation name
            runtime = float(match.group(2))
            data.append({"operation": operation, "runtime": runtime})

    return data


def collect_runtimes_from_client_logs(results_path, destination_file_path, milestone=1):
    """Recursively parse client logs and extract query runtime information.

    Args:
        results_path (str): Path to the directory containing client logs.
        destination_file_path (str): Path to save the extracted data.
    """

    all_data = []
    print(
        f"Collecting runtime data from: {results_path}; Saving to: {destination_file_path}"
    )
    # Walk through directory structure
    for dirpath, dirnames, filenames in os.walk(results_path):
        # Get data size from directory name
        param = os.path.basename(dirpath)
        param_name, value = None, None

        # in m1, param is a number
        if milestone == 1:
            if not param.isdigit():
                continue
            param_name = "data_size"
            value = int(param)

        # In m2, param is a string of format "batch_size_<batch_size>"
        if milestone == 2:
            if not param.startswith("batch_size"):
                continue
            param_name = "batch_size"
            value = int(param.split("_")[-1])

        print(f"Processing: {dirpath} for milestone {milestone}")

        # Process each test output file
        for filename in filenames:
            if filename.endswith("gen.out"):
                # Extract test number
                test_num = re.search(r"test(\d+)gen", filename)
                print(f"Processing: {filename}")
                if test_num:
                    test_num = test_num.group(1)

                    filepath = os.path.join(dirpath, filename)
                    runtime_data = parse_runtime_data(filepath)

                    # Add metadata to each operation entry
                    for entry in runtime_data:
                        entry.update({param_name: value, "test_number": int(test_num)})

                    all_data.extend(runtime_data)
    df = pd.DataFrame(all_data)
    df.to_csv(destination_file_path, index=False)

    return df


# ---------------------------- Other Utility Functions -----------------------
def report_cmd_msge(cmd, additional_info=None):
    if additional_info is None:
        additional_info = {}

    # Just to make the keys aligned
    max_key_length = max((len(key) for key in additional_info.keys()), default=0)

    print(f"{Colors.BOLD}[running]:\t {Colors.ENDC} {Colors.GREEN}{cmd}{Colors.ENDC}")

    # Print the additional information with aligned keys
    for key, value in additional_info.items():
        print(
            f"{Colors.BOLD}[{key}]:{' ' * (max_key_length - len(key))}\t "
            f"{Colors.ENDC} {Colors.CYAN}{value}{Colors.ENDC}"
        )
    print()


def show_options(options: list):
    for i, option in enumerate(options):
        print(f"\t {Colors.YELLOW}{i}. {option}{Colors.ENDC}")


def ensure_log_dir_exists(pkg_path):
    log_dir = os.path.join(pkg_path, "build_logs")
    if not os.path.exists(log_dir):
        try:
            os.makedirs(log_dir)
            print(f"Created log directory: {log_dir}")
        except OSError:
            print(f"Failed to create log directory: {log_dir}")
            return None
    return log_dir


def animate():
    for char in "|/-\\⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏":
        sys.stdout.write(
            "\r" + "Running ..." + f"{Colors.BOLD}{Colors.YELLOW}{char}{Colors.ENDC}"
        )
        sys.stdout.flush()
        time.sleep(0.1)


# ------------------------- Color Codes and Custom Help Formatter ------------
# ANSI color codes
class Colors:
    HEADER = "\033[95m"
    BLUE = "\033[94m"
    CYAN = "\033[96m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    ENDC = "\033[0m"
    BOLD = "\033[1m"
    UNDERLINE = "\033[4m"


class ColorfulHelpFormatter(argparse.HelpFormatter):
    def __init__(self, prog):
        super().__init__(prog, max_help_position=40, width=80)

    def _format_action_invocation(self, action):
        if not action.option_strings:
            default = self._get_default_metavar_for_positional(action)
            (metavar,) = self._metavar_formatter(action, default)(1)
            return metavar
        else:
            parts = []
            if action.nargs == 0:
                parts.extend(
                    f"{Colors.GREEN}{option_string}{Colors.ENDC}"
                    for option_string in action.option_strings
                )
            else:
                default = self._get_default_metavar_for_optional(action)
                args_string = self._format_args(action, default)
                for option_string in action.option_strings:
                    parts.append(
                        f"{Colors.GREEN}{option_string}{Colors.ENDC}"
                        f"{Colors.CYAN}{args_string}{Colors.ENDC}"
                    )
            return ", ".join(parts)

    def _format_usage(self, usage, actions, groups, prefix):
        formatted_usage = super()._format_usage(usage, actions, groups, prefix)
        return f"{Colors.YELLOW}{Colors.BOLD}{formatted_usage}{Colors.ENDC}"

    def _format_action(self, action):
        result = super()._format_action(action)
        return f"{Colors.BLUE}{result}{Colors.ENDC}"

    def _format_description(self, description):
        return f"{Colors.CYAN}{description}{Colors.ENDC}\n\n"
