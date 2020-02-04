#
# データ取り込みバッチ
#

# TODO DynamoDB操作処理をlib/DynamoDB_Manager.rb にすべき？

require 'aws-sdk'
require 'date'
require 'fileutils'
require 'json'
require 'optparse'

require_relative '../lib/KVS_Manager'
require_relative '../lib/ODB_Manager'

ENOUGH_ELAPSED_TIME = 3600 # 単位：秒. 60分
TMP_DIR = './tmp'

# コマンドライン引数の読み込み
def parse_ars(arv)
  config_path = nil
  opt = OptionParser.new
  opt.on('--debug', 'デバッグモード') do |v|
    $debug = true
  end
  opt.parse(arv)
end

# データ取り込みリストを取得
def get_import_list(import_list_table_name)
  data = []
  response = $dynamodb.scan(
    table_name: import_list_table_name
  )
  data = response.items

  # DynamoDBは1MB以上のデータを一度に取得できないため、
  # レスポンスに全件取得したことを意味する LastEvaluatedKey が含まれなくなるまで
  # 取得を繰り返す
  while response.include?('LastEvaluatedKey') do
    puts '[INFO] Scan DynamoDB again to get all import_list.'
    response = $dynamodb.scan(
      exclusive_start_key: response['LastEvaluatedKey']
    )
    data.extend(response.items)
  end
  # TODO 上記while処理が未テスト
  return data
end

# データ取り込みリストのkey, valueを持つitemをパース
def parse_item(item, trip_id2hex=false)
  arr = item['target_id'].split('_')
  if trip_id2hex
    return arr[0], arr[2].to_i(16), arr[1], item['last_update']
  else
    return arr[0], arr[2], arr[1], item['last_update']
  end
end

# ターゲットIDをパース
# 戻り値のフォーマット: {メインユニットID => trip_id = > [ サーバ受信日時 => last_update ]}
def parse_items(items)
  target_ids_hash = {}
  items.each do |item|
    main_unit_id, trip_id, server_received_dt, last_update = parse_item(item, trip_id2hex=true)
    target_ids_hash[main_unit_id] ||= {}
    target_ids_hash[main_unit_id][trip_id] ||= {}
    target_ids_hash[main_unit_id][trip_id][server_received_dt] = last_update
  end
  target_ids_hash
end

# 指定トリップの次のトリップIDが存在すればtruを返す。存在しなければfalseを返す
def next_trip_exists?(item, target_ids_hash)
  main_unit_id, trip_id, server_received_dt, last_update = parse_item(item, trip_id2hex=true)
  next_trip_id = trip_id + 1
  target_ids_hash[main_unit_id][next_trip_id].nil? ? false : true
end

# 指定トリップのlast_updateから現在時間までの経過時間が閾値よりも大きければtrueを返す。そうでなければfalseを返す
def enough_time?(item)
  elapsed_time =  Time.now.to_i - Time.parse(item['last_update']).to_i
  elapsed_time > ENOUGH_ELAPSED_TIME ? true : false
end

# tmpフォルダの初期化（ディレクトリの再作成）
def clear_tmp_dir
  return if $debug
  FileUtils.rm_rf(TMP_DIR)
  Dir.mkdir(TMP_DIR) unless File.exist?(TMP_DIR)
end

# メインユニットIDに紐づかないS3オブジェクトのパスリスト取得
def get_s3_object_keys()
  object_keys = []

  # 車両情報マスタ
  object_keys << [ENV['S3_LOGGER_BUCKET'], ENV['ODB_T_LOGGER_CONTAINER'], 'carinfo/carinfo.csv']

  # マスク情報リスト
  object_keys << [ENV['S3_LOGGER_BUCKET'], ENV['ODB_T_LOGGER_CONTAINER'], 'maskinfo/maskinfo.csv']

  # マスク情報
  result = $s3_client.list_objects(bucket: ENV['S3_LOGGER_BUCKET'], prefix: 'maskinfo/maskdata')
  result.contents.map do |content|
    next if content.key[-1] == '/'
    object_keys << [ENV['S3_LOGGER_BUCKET'], ENV['ODB_T_LOGGER_CONTAINER'], content.key]
  end

  # 抽出データ項目リスト
  object_keys << [ENV['S3_ETL_BUCKET'], ENV['ODB_ETL_CONTAINER'], 'conf/item_list.json']

  # 無効メインユニットIDリスト
  object_keys << [ENV['S3_ETL_BUCKET'], ENV['ODB_ETL_CONTAINER'], 'conf/invalid_car.json']

  # 車両タイプマスタ
  result = $s3_client.list_objects(bucket: ENV['S3_VIEWER_BUCKET'], prefix: 'conf/car_type.json')
  result.contents.map do |content|
    object_keys << [ENV['S3_VIEWER_BUCKET'], ENV['ODB_VIEWER_CONTAINER'], content.key]
  end

  return object_keys
end

# 指定target_idを元にtripinfoのS3オブジェクトキーを生成
def gen_s3_tripinfo_object_key(target_id)
  item = {'target_id' => target_id, 'last_update' => nil}
  main_unit_id, trip_id, server_received_dt, _ = parse_item(item)
  tripinfo_key = "tripinfo/#{main_unit_id}/#{server_received_dt}_#{trip_id}_tripinfo.csv"
  return [ENV['S3_LOGGER_BUCKET'], ENV['ODB_T_LOGGER_CONTAINER'], tripinfo_key]
end

# データ管理ファイルテーブルからtarget_idに紐づく、CAN/GPSデータのS3オブジェクトキーのリスト取得
def get_s3_can_gps_object_keys(target_id)
  object_keys = []

  result = $dynamodb.get_item(
    table_name: ENV['DYNAMODB_DATA_FILE_LIST_TABLE'],
    key: { target_id: target_id }
  )
  result.item['file_data'].map do |data|
    key = data.select{|element| element.start_with?('PATH_') }[0]
    object_keys << [ENV['S3_LOGGER_BUCKET'], ENV['ODB_T_LOGGER_CONTAINER'], key.gsub(/^PATH_/, '')]
  end

  return object_keys, result.item['file_data']
end

# 指定バケットの指定キーのオブジェクトをローカルのtmpフォルダに保存
def save_s3_objects_to_local(object_keys)
  object_keys.map do |key_arr|
    bucket_name = key_arr[0]
    key = key_arr[2]
    begin
      content = $s3_client.get_object(:bucket => bucket_name, :key => key).body.read
      outpath = TMP_DIR + '/' + bucket_name + '/' + key
      dir = File.dirname(outpath)
      FileUtils.mkdir_p(dir) unless FileTest.exist?(dir)
      File.open(outpath, "w") do |f| 
        f.puts(content)
      end
    rescue => e
      puts "[ERROR] tmpフォルダへの出力に失敗しました。" \
        + " tmp_dir= " + TMP_DIR + ", bucket_name= " + bucket_name + ", key= " + key
      puts e
      exit
    end
  end
end

# 指定されたオブジェクトキーリストを元にローカルのファイルをODBに保存
def save_local_objects_odb(object_keys)
  object_keys.map do |key_arr|
    bucket_name = key_arr[0]
    container_name = key_arr[1]
    key = key_arr[2]
    begin
      input_path = TMP_DIR + '/' + bucket_name + '/' + key
      output_path = key
      puts '[DEBUG] save_local_objects_odb() ' + container_name + ","  + key if $debug
      $odb_manager.uploadDataFileByContainer(input_path, key, container_name)
    rescue => e
      puts "[ERROR] S3からのデータ取得に失敗しました。s3_object_key= " + key + ", input_path= " + input_path + ", output_path= " + output_path
      puts e
      exit
    end
  end
end

# ターゲットIDに紐づく走行開始日時リストを取得して、KVSに保存
def import_start_time_list(target_id)
  # 走行開始日時を取得
  item = {'target_id' => target_id, 'last_update' => nil}
  main_unit_id, trip_id, server_received_dt, _ = parse_item(item)
  begin
    result = $dynamodb.get_item(
      table_name: ENV['DYNAMODB_START_TIME_LIST_TABLE'],
      key: { target_id: main_unit_id + '_' + trip_id }
    )
  rescue => e
    puts "[ERROR] 走行開始日時リストからのデータ取得に失敗しました。target_id= " + target_id
    exit
  end

  # KVSに保存
  table_name = ENV['KVS_START_TIME_LIST_TABLE']
  values = {}
  values['target_id'] = target_id
  values['start_time_data'] = result.item['start_time_data'].map{|value| value.to_f}
  # MEMO なぜか１回では登録されないので、３回実行している。
  # system()でcqlsh経由でクエリを実行すれば１回でもいけるはず。
  putDynamoTable(table_name, values)
  putDynamoTable(table_name, values)
  putDynamoTable(table_name, values)
end

# 指定されたオブジェクトキーリストをKVSのデータ管理ファイルテーブルに保存
def save_data_file_list(target_id, file_data)
  # KVSに保存するために、file_dataの文字列を整形
  table_name = ENV['KVS_DATA_FILE_LIST_TABLE']
  values = {}
  values['target_id'] = target_id
  values['file_data'] = file_data.map do |arr|
    str = arr.inject('') do |ret, val|
      ret += ',' unless ret.empty?
      ret += "'#{val}'"
      ret
    end
    "{#{str}}"
  end
  # MEMO なぜか１回では登録されないので、３回実行している。
  # system()でcqlsh経由でクエリを実行すれば１回でもいけるはず。
  putListDynamoTable(table_name, values)
  putListDynamoTable(table_name, values)
  putListDynamoTable(table_name, values)
end

# target_idを未処理リスト（KVS）に登録
def save_unprocessed_list(target_id, last_update)
  table_name = ENV['KVS_UNPROCESSED_LIST_TABLE']
  keys = 'target_id,last_update'
  values = "#{target_id}','#{last_update}"
  # MEMO なぜか１回では登録されないので、３回実行している。
  # system()でcqlsh経由でクエリを実行すれば１回でもいけるはず。
  putDynamoTableData(table_name, keys, values)
  putDynamoTableData(table_name, keys, values)
  putDynamoTableData(table_name, keys, values)
end

# 指定ターゲットIDに紐づくデータを取り込む
def import(target_id, last_update)
  # tmpフォルダ初期化（ローカルの生データ削除）
  clear_tmp_dir

  # データ管理ファイルテーブルからトリップキーに紐づくオブジェクトパスのリスト取得
  object_keys1 = get_s3_object_keys
  object_keys2 = [ gen_s3_tripinfo_object_key(target_id) ]
  object_keys3, file_data = get_s3_can_gps_object_keys(target_id)

  # 生データをtmpフォルダに出力。
  # この際、生データをtmpフォルダに出力できなかったら、終了
  save_s3_objects_to_local(object_keys1)
  save_s3_objects_to_local(object_keys2)
  save_s3_objects_to_local(object_keys3)

  # 生データをODBに保存
  save_local_objects_odb(object_keys1)
  save_local_objects_odb(object_keys2)
  save_local_objects_odb(object_keys3)

  # ターゲットIDに紐づく走行開始日時リストを取得して、KVSに保存
  import_start_time_list(target_id)

  # ターゲットIDに紐づくCAN/GPSデータのオブジェクトパスのKVSのデータ管理ファイルテーブルに保存
  save_data_file_list(target_id, file_data)

  # target_idを未処理リスト（KVS）に登録
  save_unprocessed_list(target_id, last_update)

  # tmpフォルダ初期化（ローカルの生データ削除）
  clear_tmp_dir
end

# 指定ターゲットIDをDynamoDB取り込みリストから削除
def delete_import_list(import_list_table_name, target_id)
  $dynamodb.delete_item(
    table_name: import_list_table_name,
    key: { target_id: target_id }
  )
end

def init
  # AWS
  $dynamodb = Aws::DynamoDB::Client.new
  $s3_client = Aws::S3::Client.new

  # ODB
  swift_id = ENV['SWIFT_ID']
  swift_pw = ENV['SWIFT_PW']
  auth_url = ENV['SWIFT_URL']
  $odb_manager = SwiftManager.new(auth_url, swift_id, swift_pw, \
    ENV['ODB_T_LOGGER_CONTAINER'], \
    ENV['ODB_ETL_CONTAINER'], \
    ENV['ODB_VIEWER_CONTAINER'])

  # KVS
  connectDynamo
end

def main
  init

  # データ取り込み（ターゲットIDの）リスト取得
  items = get_import_list(ENV['DYNAMODB_ON_PREMISE_IMPORT_LIST_TABLE'])
  target_ids_hash = parse_items(items)

  items.each do |item|
    # 次のトリップIDが存在せず、かつ、トリップのlast_update＋閾値 < 現在日時が成り立つ場合は
    # このトリップは収集が不完全とし、取り込み処理をスキップする
    if next_trip_exists?(item, target_ids_hash) == false && enough_time?(item) == false
      puts '[INFO] This trip data is skipped because of it does not collect completery. ' \
        + 'target_id= ' + item['target_id'] + ', last_update= ' + item['last_update']
      next
    end

    # トリップの取り込み処理
    puts '[INFO] Import this trip. target_id= ' + item['target_id'] + ', last_update= ' + item['last_update']
    import(item['target_id'], item['last_update'])

    # 現行システムDynamoDB取り込みリストから、取り込んだトリップを削除
    puts '[INFO] Delete this target_id from DynamoDB import_list.'
    delete_import_list(ENV['DYNAMODB_ON_PREMISE_IMPORT_LIST_TABLE'], item['target_id'])
  end
  closeDynamo
end

if $PROGRAM_NAME == __FILE__
  # このスクリプトが直接、実行された場合のみ、main()を実行.
  # これは他スクリプトからメソッドだけrequireしたい場合の備え
  puts '[INFO] start import.rb'
  parse_ars(ARGV)
  main
  puts '[INFO] done import.rb'
end

