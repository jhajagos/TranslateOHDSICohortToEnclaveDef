"""
    Program for converting SparkSQL generated by ATLAS/WebAPI into a format that can
    be executed in an enclave environment.

    Removes temporary table names and replaces with names that would be locally scoped

    Lower cases SQL code and then reformats to be humanreadable format

"""

import sqlparse
import re
import argparse


def process_sql(input_file_name, statements_to_replace=None):
    # Use a breakpoint in the code line below to debug your script.

    print(f"Reading '{input_file_name}'")

    internal_table_names = [
        "Codesets",
        "qualified_events",
        "inclusion_events",
        "included_events",
        "final_cohort",
        "cohort_rows",
        "strategy_ends"
    ]

    max_rules = 11
    for i in range(max_rules):
        internal_table_names += ["Inclusion_" + str(i)]

    with open(input_file_name) as f:

        sql_file = f.read()
        split_file = sqlparse.split(sql_file)
        re_table_finder = re.compile(r'(@[a-z_]+)\.([A-Za-z_0-9]+)')

        transformed_statements = []
        for statement in split_file:

            matched_table_names = re_table_finder.findall(statement)

            original_sql_statement = str(statement)

            table_list = []
            for matched_table in matched_table_names:

                internal_table = False
                for itn in internal_table_names:
                    if itn in matched_table[1]:
                        table_list += [itn]
                        internal_table = True
                        break

                if not internal_table:
                    table_list += [matched_table[1]]

            i = 0
            matched_strings = []
            for match_obj in re_table_finder.finditer(original_sql_statement):
                matched_strings += [statement[match_obj.span()[0]: match_obj.span()[1]]]
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
                modified_statement_1 = modified_statement_1.replace(match_replace[match], match)

            tables_referenced = list(match_replace)

            modified_statement_2 = modified_statement_1.lower()

            modified_statement_3 = sqlparse.format(modified_statement_2, redindent=True, keyword_case="upper")

            modified_statement_4 = modified_statement_3
            if statements_to_replace is not None:
                for st in statements_to_replace:
                    modified_statement_4 = modified_statement_4.replace(st, "")

            tables_referenced_lower = [t.lower() for t in tables_referenced]
            transformed_statements += [{"original_sql": original_sql_statement, "transformed_sql": modified_statement_4,
                                        "referenced_tables": tables_referenced_lower}]

    output_file_name = input_file_name + ".transformed.sql"
    print(f"Writing '{output_file_name}'")
    with open(output_file_name, "w") as fw:
        fw.write("/*  Translated SQL for Enclave */\n\n")
        for transformed in transformed_statements:

            fw.write(f"/* Referenced Tables: {transformed['referenced_tables']} */\n")
            fw.write(transformed["transformed_sql"])
            fw.write("\n\n")


# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    arg_parse_obj = argparse.ArgumentParser(description="Parse a Spark generated SQL File from OHDSI Atlas")

    arg_parse_obj.add_argument("-f", "--input-file-name", dest="input_file_name", default="./examples/P_Acute_Kidney_Injury_AKI_in_persons_with_chronic_kidney_disease_7Pe_7Era.sql")

    arg_obj = arg_parse_obj.parse_args()

    statements_to_replace = [
        "USING delta\n",
        "INSERT OVERWRITE TABLE codesets  (codeset_id, concept_id) "
    ]

    process_sql(arg_obj.input_file_name, statements_to_replace)

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
