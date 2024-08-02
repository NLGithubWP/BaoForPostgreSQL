import re


# Function to extract query and time
def extract_query_time(file_path):
    results = []
    with open(file_path, 'r') as file:
        lines = file.readlines()
        current_query = None
        for line in lines:
            line = line.strip()
            if line.startswith("Executing test_query_"):
                current_query = line
            elif "Time:" in line and current_query:
                match = re.search(r"Time: ([\d\.]+) ms", line)
                if match:
                    time_ms = float(match.group(1))
                    time_sec = time_ms / 1000
                    results.append((current_query, time_sec))
                current_query = None
    return results


# Function to print the results
def save_results(results, output_file):
    with open(output_file, 'w') as file:
        for query, time_sec in results:
            file.write(f"{query}: {time_sec}\n")


# Main function
def main():
    input_file_path = 'pg_run.txt'
    output_file_path = 'query_execution_times.txt'
    results = extract_query_time(input_file_path)
    save_results(results, output_file_path)
    print(f"Results saved to {output_file_path}")


if __name__ == "__main__":
    main()
