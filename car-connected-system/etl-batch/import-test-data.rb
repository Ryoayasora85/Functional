ubyスクリプトのテンプレートファイル
#

require 'optparse'
require 'fileutils'
require "aws-sdk"
require 'json'

# コマンドライン引数の読み込み
def parse_ars(arv)
  conf = {}
  opt = OptionParser.new
  opt.on('-i input_path', '入力ファイル') do |v|
    conf[:input_path] = v
  end
  opt.on('-o output_dir', '出力ディレクトリ') do |v|
    conf[:output_dir] = v
    FileUtils.mkdir_p(conf[:output_dir]) unless FileTest.exist?(conf[:output_dir])
  end
  opt.parse(arv)
#raise 'must use -i option' if conf[:input_path].nil?
#raise 'ust use -o option' if conf[:output_dir].nil?
  return conf
end

#######################
#                     #
# スクリプトここから  #
#                     #
#######################

#テーブルにテストカラム

def put_test_item_list(table_name,dynamodb,json_data) 

  json_data.each_with_index {|col,i|
  dynamodb.put_item(
    table_name: table_name,
    item: {
      target_id: "#{col[0]}",
      last_update: "#{col[1]}"
   }
  ) 
 }

end

#テーブルの取り込みリスト取得
def get_test_item_list(table_name,dynamodb)
  result = dynamodb.scan(
    table_name: table_name
  )
  return result.items
end

#last_updateのitemを取得
def get_item(key,value,table_name,dynamodb)
  result = dynamodb.get_item(
    table_name: table_name,
    key: {
      target_id: value
    }
  )
#itemの中身がからなら戻り値を返さない
  if result.item.nil? == false
  return result.item["last_update"]
  end
end

#######################
#                     #
# 　メインここから 　 #
#                     #
#######################

def main(conf)
#テーブルの定義
  table_name = 'test_on_premise_import_list'
  json_name = conf[:input_path]
  dynamodb = Aws::DynamoDB::Client.new
#jsonデータの取り込み
  json_data = JSON.parse(File.read(json_name))
#テーブルにテストカラム
  result = put_test_item_list(table_name,dynamodb,json_data)
  #puts result

#dynamodbにテストトリップが登録されているか確認
#スキャンして登録されているレコード数を取得する
#登録するjsonファイルのレコードとスキャンしたレコードの数が一致してるかどうか比較する
#比較結果でfalseだった場合とtrueだった場合の表示をさせる



#取込みリスト取得
  #items = get_test_item_list(table_name,dynamodb)
  #puts items
#last_updateのitemを取得
  #getItem = get_item(key,value,table_name,dynamodb)
end



if $PROGRAM_NAME == __FILE__
# このスクリプトが直接、実行された場合のみ、main()を実行.
# これは他スクリプトからメソッドだけrequireしたい場合の備え
  conf = parse_ars(ARGV)
  main(conf)
  puts 'done'
end


