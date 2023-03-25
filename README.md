Snowflake Custom Query Profiler
===============================

Simple tool to connect to Snowflake and generate a HTML file with a DOT graph that shows the query profile for a query ID.

# Database Profile File

Create a **profiles_db.conf** copy of the **profiles_db_template.conf** file, and customize it with your own Snowflake connection parameters: just the account name and user name. Your top [default] profile is the active profile, considered by our tool. Below you may define other personal profiles, that you may override under [default] each time you want to change your active connection.

We connect to Snowflake with the Snowflake Connector for Python. We have code for (a) password-based connection, (b) connecting with a Key Pair, and (c) connecting with SSO. For password-based connection, save your password in a SNOWFLAKE_PASSWORD local environment variable. Never add the password or any other sensitive information to your code or to profile files. All names must be case sensitive, with no quotes.

# CLI Executable File

To compile into a CLI executable:

**<code>pip install pyinstaller</code>**  
**<code>pyinstaller --onefile custom-query-profiler.py</code>**  
**<code>dist\custom-query-profiler</code>**  

# Show Some Query Execution Profile

Run a query from Snowflake and copy its query ID, after the execution. From the command line, make sure you replace the last argument with your own query ID:

**<code>python custom-query-profiler.py 01ab2f03-0502-b9aa-004e-a2830079d89e</code>**  

I ran the following query:

```
select c_name, c_custkey, o_orderkey, o_orderdate, 
  o_totalprice, sum(l_quantity)
from customer
  inner join orders on c_custkey = o_custkey
  inner join lineitem on o_orderkey = l_orderkey
where o_orderkey in (
    select l_orderkey
    from lineitem
    group by l_orderkey
    having sum(l_quantity) > 200)
  and o_orderdate >= dateadd(year, -25, current_date)
group by c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice 
order by o_totalprice desc, o_orderdate;
```

And here is the top portion of my complex query execution plan:

![Top Portion of Custom Query Profile](/images/diagram1.png)

The middle portion:

![Middle Portion of Custom Query Profile](/images/diagram2.png)

The bottom portion:

![Bottom Portion of Custom Query Profile](/images/diagram3.png)

# The new GET_QUERY_OPERATOR_STATS table system function

The info was collected from the result of the following call (replace with your own query ID):

**<code>select * from table(GET_QUERY_OPERATOR_STATS('01ab2f03–0502-b9aa-004e-a2830079d89e'))</code>**  

![GET_QUERY_OPERATOR_STATS Results](/images/diagram_results.png)

# The Query Profile in Snowflake

Two images with the similar built-in Query Profiler from Snowflake, for the exact same query:

![Query Profile in Snowflake](/images/query_profile.png)

Another partial view:

![Query Profile in Snowflake - Another View](/images/query_profile2.png)

