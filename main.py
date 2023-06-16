# Parsing code

import sqlparse
import re
import argparse


def process_sql(input_file_name):
    # Use a breakpoint in the code line below to debug your script.

    with open(input_file_name) as f:
        sql_file = f.read()

        split_file = sqlparse.split(sql_file)

        print(split_file)

        re_table_finder = re.compile(r'(@[a-z_]+)\.([a-z0-9]+ap)?([A-Za-z_]+)')

        transformed_statements = []
        for statement in split_file:
            matched_table_names = re_table_finder.findall(statement)

            original_sql_statement = statement

            table_list = []
            for matched_table in matched_table_names:
                table_list += [matched_table[2]]

            i = 0
            matched_strings = []
            for match_obj in re_table_finder.finditer(statement):
                matched_strings += [ statement[match_obj.span()[0] : match_obj.span()[1]]]
                i += 1

            matches_list = list(zip(table_list, matched_strings))

            match_count = {}
            match_replace = {}
            for match in matches_list:
                if match[0] in match_count:
                    match_count[match[0]] += 1
                else:
                    match_count[match[0]] = 1
                    match_replace[match[0]] = match[1]

            modified_statement_1 = statement
            for match in match_replace:
                modified_statement_1 = modified_statement_1.replace(match_replace[match], match, match_count[match])

            tables_referenced = list(match_replace)

            modified_statement_2 = modified_statement_1.replace("USING DELTA\n", "")

            modified_statement_3 = modified_statement_2.lower()

            modified_statement_4 = sqlparse.format(modified_statement_3, redindent=True, keyword_case="upper")

            transformed_statements += [{"original_sql": original_sql_statement, "transformed_sql": modified_statement_4, "referenced_tables": tables_referenced}]

    output_file_name = input_file_name + ".transformed.sql"
    with open(output_file_name, "w") as fw:
        fw.write("/*  Translated SQL for Enclave */\n\n")
        for transformed in transformed_statements:

            fw.write(f"/* Referenced Tables: {transformed['referenced_tables']} */\n")
            fw.write(transformed["transformed_sql"])
            fw.write("\n\n")






# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    arg_parse_obj = argparse.ArgumentParser(description="Parse a Spark generated SQL File from OHDSI Atlas")

    arg_parse_obj.add_argument("-f", "--input-file-name", dest="input_file_name", default="./examples/cure_id_spark_atlas_v.sql")

    arg_obj = arg_parse_obj.parse_args()

    process_sql(arg_obj.input_file_name)

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
