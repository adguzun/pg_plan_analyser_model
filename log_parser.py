import re
import hashlib
import json
from pathlib import Path
from pprint import pprint

# Path to the log file
log_file_path = "/Users/denisguzun/Downloads/1234.log" # Update this to your log file path

# Define regex patterns for parsing
duration_pattern = re.compile(r"LOG:\s+duration:\s+([\d\.]+)\s+ms")
query_pattern = re.compile(r"Query Text:\s*(.*)")
plan_line_pattern = re.compile(
    r"(\w+)\s+\(cost=([\d\.]+)\.\.([\d\.]+)\s+rows=(\d+)\s+width=(\d+)\)"
    r"(?:\s+\(actual time=([\d\.]+)\.\.([\d\.]+)\s+rows=(\d+)\s+loops=(\d+)\))?"
)

# Initialize a list to store parsed plans
plans = []
current_plan = None
expecting_plan_lines = False  # Flag to determine if we are reading plan lines

# Output JSON file name (similar to the input file)
output_file_path = Path(log_file_path).with_suffix(".json")


def normalize_sql(sql_text):
    """
    Normalize the SQL text by removing extra whitespace and converting to lowercase.
    """
    return " ".join(sql_text.lower().split())


with open(log_file_path, "r") as file:
    for line in file:
        try:
            # Match the LOG line with duration
            duration_match = duration_pattern.search(line)
            if duration_match:
                # Save the current plan if it exists
                if current_plan:
                    plans.append(current_plan)
                # Start a new plan
                current_plan = {
                    "duration_ms": float(duration_match.group(1)),
                    "query": None,
                    "query_hash": None,
                    "operations": []
                }
                continue

            # Match the Query Text
            query_match = query_pattern.search(line)
            if query_match and current_plan:
                sql_text = query_match.group(1)
                current_plan["query"] = sql_text
                # Normalize and calculate the hash of the query text
                normalized_sql = normalize_sql(sql_text)
                current_plan["query_hash"] = hashlib.sha256(normalized_sql.encode()).hexdigest()
                expecting_plan_lines = True  # Start expecting plan lines
                continue

            # Capture plan operation lines
            if expecting_plan_lines and current_plan:
                plan_line_match = plan_line_pattern.search(line)
                if plan_line_match:
                    operation = plan_line_match.group(1)
                    cost_start = float(plan_line_match.group(2))
                    cost_end = float(plan_line_match.group(3))
                    rows = int(plan_line_match.group(4))
                    width = int(plan_line_match.group(5))
                    actual_time_start = (
                        float(plan_line_match.group(6)) if plan_line_match.group(6) else None
                    )
                    actual_time_end = (
                        float(plan_line_match.group(7)) if plan_line_match.group(7) else None
                    )
                    actual_rows = (
                        int(plan_line_match.group(8)) if plan_line_match.group(8) else None
                    )
                    loops = (
                        int(plan_line_match.group(9)) if plan_line_match.group(9) else None
                    )

                    current_plan["operations"].append(
                        {
                            "operation": operation,
                            "cost_start": cost_start,
                            "cost_end": cost_end,
                            "rows": rows,
                            "width": width,
                            "actual_time_start": actual_time_start,
                            "actual_time_end": actual_time_end,
                            "actual_rows": actual_rows,
                            "loops": loops,
                        }
                    )
                elif line.strip() == "":
                    # Empty line indicates the end of a plan section
                    expecting_plan_lines = False
        except Exception as e:
            print(f"Error processing line: {line.strip()}")
            print(f"Details: {e}")

# Append the last parsed plan
if current_plan:
    plans.append(current_plan)

# Save the parsed plans to a JSON file
with open(output_file_path, "w") as output_file:
    json.dump(plans, output_file, indent=4)

# Print the output file path for confirmation
pprint(f"Results saved to: {output_file_path}")
