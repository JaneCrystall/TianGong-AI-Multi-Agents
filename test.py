import os


concatenated_result = "jahgaliogh;aoeir"


markdown_file_name = 'summary_output.md'

# Save the model's response to a Markdown file
with open(markdown_file_name, 'w') as markdown_file:
    markdown_file.write(concatenated_result)