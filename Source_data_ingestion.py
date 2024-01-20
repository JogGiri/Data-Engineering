def oracle_conn_config(host,port,db,username,password):
  jdbcUrl = "jdbc:oracle:thin:@//{0}:{1}/{2}".format(host, port, db)
  connectionProperties = {
    "user" : username,
    "password" : password,
    "driver" : "oracle.jdbc.driver.OracleDriver",
    "oracle.jdbc.timezoneAsRegion" : "false",
    "fetchsize":"100000"}
  return (jdbcUrl,connectionProperties)


def sqlserver_conn_config(host,port,db,username,password):
    jdbc_url = "jdbc:sqlserver://{0}:{1};databaseName={2};user={3};password={4}".format(host, port, db, username, password)
    connection_properties = {
        "user" : username,
        "password" :password,
        "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"}
    return (jdbc_url,connection_properties)  

def db2_conn_config(host, username, password):
    url = "jdbc:as400://"+host
    prop = {
        "user": username, 
        "password": password, 
        "driver": "com.ibm.as400.access.AS400JDBCDriver", 
        "sslConnection": "true"}
    return (url, prop)

def progress_conn_config(host,port,db,username,password):
    jdbcUrl = f"jdbc:datadirect:openedge://{host}:{port};databaseName={db};truncateTooLarge=output;allowExceptionForInvalidEncodedData=false"
    connectionProperties = {
      "user" : username,
      "password" : password,
      "driver" : "com.ddtek.jdbc.openedge.OpenEdgeDriver",
      "fetchsize":"100000"}
    return (jdbcUrl,connectionProperties)


  
### Snowflake Connections
  def snowflake_conn_config(host,username,password,schema,database,account):  
      options = {
        "sfUrl":host,
        "sfUser":username,
          "sfPassword":password,
          "sfWarehouse":"",
          "sfDatabase":database,
          "sfSchema":schema,
          "sfRole" : "",
          "sfAccount":""
      }
      return options


  ###Postgres connection
  def postgres_conn():
    try:
      conn = psycopg2.connect(user=postgres_user,password=postgres_passwd, host=postgres_host, port=postgres_port,database=postgres_database)
      
      if conn:
        print("PostGres Connection is Successful.")
      else:
        logging.info("Postgres connection failed.")
        
      return conn
    except Exception as e:
      print("Process Failed while connecting to Postgres -> " + str(e))
      sys.exit(1)
      
  def execute_query(query):
    try:
      conn=connections.postgres_conn()
      cursor = conn.cursor()
      print("Query to be executed is " + query)
      cursor.execute(query)
      print("Query is executed in Postgres")
      conn.commit()
      cursor.close()
      conn.close()
    except Exception as e:
      print(f"Process Failed while executing query -> {query} " + str(e))
      sys.exit(1)
