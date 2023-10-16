import csv

input_file = 'todos.csv'
output_file_prefix = 'todos_chunk_'
lines_per_file = 8000

with open(input_file, 'r') as infile:
    reader = csv.reader(infile)
    header = next(reader)  # Store the header for later use
    
    file_count = 1
    current_line = 0
    writer = None
    
    for row in reader:
        if current_line % lines_per_file == 0:
            if writer:
                outfile.close()
            
            output_file = f'{output_file_prefix}{file_count}.csv'
            outfile = open(output_file, 'w', newline='')
            writer = csv.writer(outfile)
            writer.writerow(header)  # Write the header to each output file
            file_count += 1

        writer.writerow(row)
        current_line += 1

    if writer:
        outfile.close()

