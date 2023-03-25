"""
Created By:    Cristian Scutaru
Creation Date: Mar 2023
Company:       XtractPro Software
"""

import os, sys, json
from html import escape
import configparser
import snowflake.connector
from pathlib import Path
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

def getDot(objects, cur, queryId):
    """
    generates and returns a graph in DOT notation
    """

    nodes = ""; edges = ""
    query = f"select * from table(GET_QUERY_OPERATOR_STATS('{queryId}'))"
    results = cur.execute(query).fetchall()
    for row in results:
        nodeId = str(row[2])
        parentId = row[3]
        step = str(row[4])
        nodes += (f'  n{nodeId} [\n'
            + f'    style="filled" shape="record" color="SkyBlue"\n'
            + f'    fillcolor="#d3dcef:#ffffff" color="#716f64" penwidth="1"\n'
            + f'    label=<<table style="rounded" border="0" cellborder="0" cellspacing="0" cellpadding="1">\n'
            + f'      <tr><td bgcolor="transparent" align="center"><font color="#000000"><b>{step}</b></font></td></tr>\n')

        oper = json.loads(str(row[7]))
        if 'table_name' in oper:
            nodes += f'      <tr><td align="left"><font color="#000000">table_name: {oper["table_name"]}</font></td></tr>\n'
        #if 'filter_condition' in oper:
        #    nodes += f'      <tr><td align="left"><font color="#000000">filter_condition: {escape(oper["filter_condition"])}</font></td></tr>\n'
        if 'join_type' in oper:
            nodes += f'      <tr><td align="left"><font color="#000000">join_type: {oper["join_type"]}</font></td></tr>\n'
        if 'join_id' in oper:
            edges += f'  n{nodeId} -> n{oper["join_id"]} [  dir="forward" style="dashed" ];\n'

        execTime = json.loads(str(row[6]))
        if 'overall_percentage' in execTime:
            overall_percentage = float(execTime['overall_percentage'])
            if overall_percentage  > 0.0:
                nodes += f'      <tr><td align="left"><font color="#000000">overall_percentage: {"{0:.1%}".format(overall_percentage)}</font></td></tr>\n'
            if 'remote_disk_io' in execTime:
                nodes += f'      <tr><td align="left"><font color="#000000">remote_disk_io: {"{0:.0%}".format(execTime["remote_disk_io"])}</font></td></tr>\n'

        stats = json.loads(str(row[5]))
        if 'io' in stats:
            io = stats["io"]
            if 'bytes_scanned' in io:
                nodes += f'      <tr><td align="left"><font color="#000000">bytes_scanned: {io["bytes_scanned"]:,}</font></td></tr>\n'
            if 'percentage_scanned_from_cache' in io:
                nodes += f'      <tr><td align="left"><font color="#000000">percentage_scanned_from_cache: {"{0:.2%}".format(io["percentage_scanned_from_cache"])}</font></td></tr>\n'
            if 'bytes_written_to_result' in io:
                nodes += f'      <tr><td align="left"><font color="#000000">bytes_written_to_result: {io["bytes_written_to_result"]:,}</font></td></tr>\n'

        if 'pruning' in stats:
            nodes += f'      <tr><td align="left"><font color="#000000">partitions_scanned: {stats["pruning"]["partitions_scanned"]}</font></td></tr>\n'
            nodes += f'      <tr><td align="left"><font color="#000000">partitions_total: {stats["pruning"]["partitions_total"]}</font></td></tr>\n'

        nodes += f'    </table>>\n  ]\n'

        if 'input_rows' in stats:
            nodes += f'  i{nodeId} [ label="{stats["input_rows"]:,}" style="filled" shape="oval" fillcolor="#ffffff" ]\n'
            edges += f'  n{nodeId} -> i{nodeId};\n'

        if parentId != None:
            edges += f'  i{parentId} -> n{nodeId};\n'

    return ('digraph G {\n'
        + f'  graph [ rankdir="TB" bgcolor="#ffffff" ]\n'
        + f'  edge [ penwidth="1" color="#696969" dir="back" style="solid" ]\n\n'
        + f'{nodes}\n{edges}}}\n')

def saveHtml(filename, s):
    """
    save in HTML file using d3-graphviz
    https://bl.ocks.org/magjac/4acffdb3afbc4f71b448a210b5060bca
    https://github.com/magjac/d3-graphviz#creating-a-graphviz-renderer
    """
    s = ('<!DOCTYPE html>\n'
        + '<meta charset="utf-8">\n'
        + '<body>'
        + '<script src="https://d3js.org/d3.v5.min.js"></script>\n'
        + '<script src="https://unpkg.com/@hpcc-js/wasm@0.3.11/dist/index.min.js"></script>\n'
        + '<script src="https://unpkg.com/d3-graphviz@3.0.5/build/d3-graphviz.js"></script>\n'
        + '<div id="graph" style="text-align: center;"></div>\n'
        + '<script>\n'
        + 'var graphviz = d3.select("#graph").graphviz()\n'
        + '   .on("initEnd", () => { graphviz.renderDot(d3.select("#digraph").text()); });\n'
        + '</script>\n'
        + '<textarea id="digraph" style="display:none; height:0px;">\n'
        + s
        + '</textarea></body>\n')

    print(f"Generating {filename} file...")
    with open(filename, "w") as file:
        file.write(s)

def connect(connect_mode, account, user):

    # (a) connect to Snowflake with SSO
    if connect_mode == "SSO":
        return snowflake.connector.connect(
            account = account,
            user = user,
            authenticator = "externalbrowser"
        )

    # (b) connect to Snowflake with username/password
    if connect_mode == "PWD":
        return snowflake.connector.connect(
            account = account,
            user = user,
            password = os.getenv('SNOWFLAKE_PASSWORD')
        )

    # (c) connect to Snowflake with key-pair
    if connect_mode == "KEY-PAIR":
        with open(f"{str(Path.home())}/.ssh/id_rsa_snowflake_demo", "rb") as key:
            p_key= serialization.load_pem_private_key(
                key.read(),
                password = None, # os.environ['SNOWFLAKE_PASSPHRASE'].encode(),
                backend = default_backend()
            )
        pkb = p_key.private_bytes(
            encoding = serialization.Encoding.DER,
            format = serialization.PrivateFormat.PKCS8,
            encryption_algorithm = serialization.NoEncryption())

        return snowflake.connector.connect(
            account = account,
            user = user,
            private_key = pkb
        )

def main():
    """
    Main entry point of the CLI
    """

    # read profiles_db.conf
    parser = configparser.ConfigParser()
    parser.read("profiles_db.conf")
    section = "default"
    account = parser.get(section, "account")
    user = parser.get(section, "user")

    # Query ID must be passed in the command line
    if len(sys.argv) < 2:
        print("You must pass a valid Query ID in the command line!")
        sys.exit(2)
    queryId = sys.argv[1]

    # change this to connect in a different way: SSO / PWD / KEY-PAIR
    connect_mode = "PWD"
    con = connect(connect_mode, account, user)
    cur = con.cursor()

    # get DOT digraph string
    objects = []
    s = getDot(objects, cur, queryId)
    print("\nGenerated DOT digraph:")
    print(s)
    con.close()

    # save as HTML file
    filename = f"output/{account}.html"
    saveHtml(filename, s)

if __name__ == "__main__":
    main()
