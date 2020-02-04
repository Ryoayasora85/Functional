######################
# 処理が同時に複数 走らないようにする。
#
# すでに処理が走っていれば、その旨を表示して exit 1 する。
# 他に処理が走っていなければ、 処理を行う。
# 処理が正常処理したなら、exit 0 する。
# 処理が正常終了しなければ exit 1 する。
######################

require 'rspec'
require './single_task.rb'

describe 'apas_server' do

  def sample_task(s = 1)
    puts "### sleep #{s} ..."
    sleep s
    0
  end

  specify "run normal" do
    ret = sample_task 1
    expect(ret).to eq(0)
  end

  specify "run as single_task" do
    ret = single_run { sample_task 1 }
    expect(ret).to eq(0)
  end

  specify "run multi as single_task" do
    t = Thread.new do
      ret = single_run { sample_task 3 }
      expect(ret).to eq(0)
      # 上の処理が終わったあとなら、正常に実行できる。
      ret = single_run { sample_task 1 }
      expect(ret).to eq(0)
    end

    # 上の処理が実行中なので、 ret = 1 になる。
    ret = single_run { sample_task 1 }
    expect(ret).to eq(1)

    t.join
  end

  # TODO: Ctrl-C, kill などシグナルが送られた時の挙動をテストする事。

end

# # DB接続用オブジェクト作成
# #
# def connectDynamo()
#
# #  $dynamoClient = Aws::DynamoDB::Client.new(
# #    :region => "ap-northeast-1",
# #    :access_key_id => ENV['AWS_ID'],
# #    :secret_access_key => ENV['AWS_KEY']
# #  )
# #  return $dynamoClient
#
#   ###########################################
#   # 環境変数および設定ファイル読込
#   ###########################################
#   kvs_host = ENV['KVS_HOST']
#   kv_keyspace = ENV['KVS_KEYSPACE']
#   @kvs_debug = ENV['KVS_DEBUG']
#
#
# # puts @kvs_debug
#
#   if @kvs_debug == "ON"
#     puts "@kvs_debug = ON Puts debug INFO"
#     puts kvs_host
#     puts kv_keyspace
#   end
#
#   @cluster = Cassandra.cluster(hosts: [kvs_host])
#   @cluster.each_host do |host| # automatically discovers all peers
#     if @kvs_debug == "ON"
#        puts "Host #{host.ip}: id=#{host.id} datacenter=#{host.datacenter} rack=#{host.rack}"
#     end
#   end
#
#   @keyspace = kv_keyspace
#   @session  = @cluster.connect(@keyspace) # create session, optionally scoped to a keyspace, to execute queries
# #  return $session
# end
#
# #
# # テーブルデータ全件取得(Scan)
# #
# def getDynamoTableDataAll(tableName, keyId)
# #
# #  outList = Array.new
# #  startIndex = nil
# #  while true
# #    # 次ページデータがあった場合、start_keyを指定して全件取得
# #    if startIndex
# #      scan_list = $dynamoClient.scan(
# #        table_name: tableName,
# #        consistent_read: true,
# #        exclusive_start_key: { keyId => startIndex[keyId]}
# #      )
# #    # 最初はstart_key指定無しで全件取得
# #    else
# #      scan_list = $dynamoClient.scan(
# #        consistent_read: true,
# #        table_name: tableName
# #      )
# #    end
# #    outList.concat(scan_list.items)
# #    # 次ページデータがあった場合、保存
# #    if scan_list.last_evaluated_key
# #      startIndex = scan_list.last_evaluated_key
# #    else
# #      break
# #    end
# #  end
# #
# #  return outList
# #
# #  outList = $session.execute_async("SELECT * FROM #{tableName} ") # fully asynchronous api
# #  outList = $session.execute("SELECT * FROM #{tableName} ") # fully asynchronous api
#    query = "SELECT * FROM #{tableName} "
#    if @kvs_debug == "ON"
#
#          puts "FUNC:#{self.method_name}"
#       puts query
#    end
#    outList = @session.execute(query)
#
# #   outList.on_success do |rows|
# #    rows.each do |row|
# #      puts "The keyspace #{row['test_keyspace']} has a table called #{row['target_id']}"
# #      outList2.concat(rows)
# #    end
#
#   return outList
#
# end

