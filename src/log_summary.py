import os, re, argparse, traceback
from threading import Lock
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict

def process_file(file_path, error_codes, error_messages, lock_c, lock_m):
    error_c_groups = defaultdict(lambda: 0)
    error_m_groups = defaultdict(lambda: 0)

    with open(file_path, "r") as file:
        for line in file:
            pattern = re.search(r'^[0-9-]+ [0-9:]+ \[([A-Z]+)\] (.*?)$', line)
            code = pattern.group(1)
            msg = pattern.group(2)
            error_c_groups[code] += 1
            error_m_groups[msg] += 1
            
    with lock_c:
        for key, val in error_c_groups.items():
            error_codes[key] += val

    with lock_m:
        for key, val in error_m_groups.items():
            error_messages[key] += val

def to_csv(out_dir, error_codes, error_messages):
    error_codes = dict(sorted(error_codes.items(), key = lambda item: item[1], reverse = True))
    error_messages = dict(sorted(error_messages.items(), key = lambda item: item[1], reverse = True))
    if out_dir[-1] != "/":
        out_dir += "/"

    error_messages_csv = "Error Message,Count\n"
    for key, val in list(error_messages.items())[:5]:
        error_messages_csv += f"{key},{val}\n"

    with open(out_dir + "error_messages_summary.csv", "w") as file:
        file.write(error_messages_csv)

    error_codes_csv = "Error Message,Count\n"
    for key, val in list(error_codes.items())[:5]:
        error_codes_csv += f"{key},{val}\n"

    with open(out_dir + "error_codes_summary.csv", "w") as file:
        file.write(error_codes_csv)

def main():
    parser = argparse.ArgumentParser(description = "Summarize log files.")
    parser.add_argument("-p", "--path", type = str, nargs = "+", required = True, help = "Wildcard for log files")
    parser.add_argument("-t", "--threads", type = int, default = os.cpu_count(), help = "Number of threads to use")
    parser.add_argument("-o", "--out_dir", type = str, default = "../", help = "Output directory")
    args = parser.parse_args()

    error_codes = defaultdict(lambda: 0)
    error_messages = defaultdict(lambda: 0)

    lock_c = Lock()
    lock_m = Lock()

    with ThreadPoolExecutor(max_workers = args.threads) as executor:
        futures = [
            executor.submit(process_file, file_path, error_codes, error_messages, lock_c, lock_m)
            for file_path in args.path
        ]
        
        for future in futures:
            future.result()

    to_csv(args.out_dir, error_codes, error_messages)

if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception:
        print(traceback.format_exc())