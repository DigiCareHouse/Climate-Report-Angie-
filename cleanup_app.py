import os

file_path = 'app.py'
with open(file_path, 'r', encoding='utf-8') as f:
    lines = f.readlines()

new_lines = []
skip = False
found_start = False
found_end = False
deleted_count = 0

for line in lines:
    # Start of deletion block: Look for '    saved_files = []' (4 spaces)
    # The new code uses 8 spaces because it's inside a try block
    if not found_start and line.rstrip() == '    saved_files = []':
        print(f"Found deletion start at line: {line.strip()}")
        skip = True
        found_start = True
    
    # End of deletion block
    if skip and 'return redirect(url_for("download_file", filename=out_name))' in line:
        print(f"Found deletion end at line: {line.strip()}")
        skip = False
        found_end = True
        continue # Skip the end line itself

    if not skip:
        new_lines.append(line)
    else:
        deleted_count += 1

if found_start and found_end:
    with open(file_path, 'w', encoding='utf-8') as f:
        f.writelines(new_lines)
    print(f"Cleanup successful. Deleted {deleted_count} lines.")
else:
    print(f"Cleanup failed. Start found: {found_start}, End found: {found_end}")
