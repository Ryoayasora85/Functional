テーブルデータ全件取得(Scan)

def getDynamoTableDataAll(tableName, keyId)

 outList = Array.new
 startIndex = nil
 while true
    次ページデータがあった場合、start_keyを指定して全件取得
   if startIndex
     scan_list = $dynamoClient.scan(
       table_name: tableName,
       consistent_read: true,
       exclusive_start_key: { keyId => startIndex[keyId]}
     )
    最初はstart_key指定無しで全件取得
   else
     scan_list = $dynamoClient.scan(
       consistent_read: true,
       table_name: tableName
     )
   end
   outList.concat(scan_list.items)
    次ページデータがあった場合、保存
   if scan_list.last_evaluated_key
     startIndex = scan_list.last_evaluated_key
   else
     break
   end
 end

 return outList

 outList = $session.execute_async("SELECT * FROM {tableName} ")  fully asynchronous api
 outList = $session.execute("SELECT * FROM {tableName} ")  fully asynchronous api
  query = "SELECT * FROM {tableName} "
  if @kvs_debug == "ON"

        puts "FUNC:{self.method_name}"
     puts query
  end
  outList = @session.execute(query)

  outList.on_success do |rows|
   rows.each do |row|
     puts "The keyspace {row['test_keyspace']} has a table called {row['target_id']}"
     outList2.concat(rows)
   end

 return outList

end

