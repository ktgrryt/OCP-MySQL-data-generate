import mysql.connector
import random
import time
import string
from datetime import datetime
# データベース接続情報
import os
DB_CONFIG = {
    'host': os.environ.get('DB_HOST', 'mysql-service'),
    'user': os.environ.get('DB_USER', 'username'),
    'password': os.environ.get('DB_PASSWORD', 'password'),
    'database': os.environ.get('DB_NAME', 'dbname'),
    # 接続設定の最適化 - サポートされているパラメータのみ使用
    'connect_timeout': 60,
    'connection_timeout': 180,
    'pool_size': 5,
    'pool_reset_session': True
    # cmd_timeout は削除しました
}
# 乱数の種を現在時刻で初期化
random.seed(datetime.now().timestamp())
# ブランド名生成のためのコンポーネント
brand_prefixes = [
    "Neo", "Ultra", "Mega", "Hyper", "Dyna", "Eco", "Tech", "Smart", "Pure", "Pro",
    "Zen", "Evo", "Nova", "Alpha", "Beta", "Delta", "Gamma", "Sigma", "Omega", "Prime"
]
brand_roots = [
    "Tech", "Soft", "Wave", "Vision", "Style", "Craft", "Logic", "Mind", "Sense", "Core",
    "Link", "Sync", "Flow", "Pixel", "Byte", "Data", "Forge", "Labs", "Ware", "Gadget"
]
brand_suffixes = [
    "Corp", "Inc", "Co", "Ltd", "Group", "Systems", "Works", "Industries", "Solutions", "Global",
    "Labs", "Tech", "Networks", "Dynamics", "Innovations", "Designs", "Interactive", "Digital", "Ventures", "Enterprises"
]
# 100個のカテゴリ定義
categories_list = [
    # 電子機器・IT関連 (25個)
    "スマートフォン", "タブレット", "ノートPC", "デスクトップPC", "スマートウォッチ",
    "ワイヤレスイヤホン", "ヘッドホン", "スピーカー", "テレビ", "モニター",
    "ゲーム機", "カメラ", "ビデオカメラ", "プロジェクター", "プリンター",
    "スキャナー", "外付けHDD", "SSD", "USBメモリ", "マウス",
    "キーボード", "ルーター", "ドローン", "VRヘッドセット", "電子書籍リーダー",
    
    # 家電 (20個)
    "冷蔵庫", "洗濯機", "電子レンジ", "オーブン", "炊飯器",
    "掃除機", "空気清浄機", "加湿器", "除湿機", "エアコン",
    "ドライヤー", "アイロン", "コーヒーメーカー", "ミキサー", "トースター",
    "食器洗い機", "電気ポット", "ホットプレート", "扇風機", "電気毛布",
    
    # 家具・インテリア (15個)
    "ソファ", "椅子", "テーブル", "ベッド", "デスク",
    "本棚", "食器棚", "タンス", "クローゼット", "テレビ台",
    "照明", "カーテン", "カーペット", "クッション", "観葉植物",
    
    # 衣類・ファッション (15個)
    "Tシャツ", "シャツ", "パンツ", "ジーンズ", "スカート",
    "ドレス", "スーツ", "コート", "ジャケット", "セーター",
    "カーディガン", "パーカー", "靴下", "下着", "帽子",
    
    # 食品・飲料 (10個)
    "チョコレート", "クッキー", "アイスクリーム", "スナック", "ドリンク",
    "調味料", "パスタ", "米", "パン", "シリアル",
    
    # スポーツ・アウトドア (10個)
    "テニスラケット", "ゴルフクラブ", "サッカーボール", "バスケットボール", "野球グローブ",
    "ランニングシューズ", "自転車", "テント", "寝袋", "登山リュック",
    
    # 美容・健康 (5個)
    "化粧品", "香水", "スキンケア", "サプリメント", "マッサージ機"
]
# カテゴリ系統の定義（既存の商品テンプレートで使用）
category_systems = {
    "電子機器": [
        "スマートフォン", "タブレット", "ノートPC", "デスクトップPC", "スマートウォッチ",
        "ワイヤレスイヤホン", "ヘッドホン", "スピーカー", "テレビ", "モニター",
        "ゲーム機", "カメラ", "ビデオカメラ", "プロジェクター", "プリンター",
        "スキャナー", "外付けHDD", "SSD", "USBメモリ", "マウス",
        "キーボード", "ルーター", "ドローン", "VRヘッドセット", "電子書籍リーダー"
    ],
    "家電": [
        "冷蔵庫", "洗濯機", "電子レンジ", "オーブン", "炊飯器",
        "掃除機", "空気清浄機", "加湿器", "除湿機", "エアコン",
        "ドライヤー", "アイロン", "コーヒーメーカー", "ミキサー", "トースター",
        "食器洗い機", "電気ポット", "ホットプレート", "扇風機", "電気毛布"
    ],
    "家具": [
        "ソファ", "椅子", "テーブル", "ベッド", "デスク",
        "本棚", "食器棚", "タンス", "クローゼット", "テレビ台",
        "照明", "カーテン", "カーペット", "クッション", "観葉植物"
    ],
    "衣類": [
        "Tシャツ", "シャツ", "パンツ", "ジーンズ", "スカート",
        "ドレス", "スーツ", "コート", "ジャケット", "セーター",
        "カーディガン", "パーカー", "靴下", "下着", "帽子"
    ],
    "食品": [
        "チョコレート", "クッキー", "アイスクリーム", "スナック", "ドリンク",
        "調味料", "パスタ", "米", "パン", "シリアル"
    ],
    "スポーツ": [
        "テニスラケット", "ゴルフクラブ", "サッカーボール", "バスケットボール", "野球グローブ",
        "ランニングシューズ", "自転車", "テント", "寝袋", "登山リュック"
    ],
    "美容": [
        "化粧品", "香水", "スキンケア", "サプリメント", "マッサージ機"
    ]
}
# 商品名と説明文のテンプレート（日本語文法修正版）
product_templates = {
    "電子機器": {
        "name_templates": [
            "{brand_short} {category}{model}",
            "{brand_short} {category} {series}{model}",
            "{brand_short} {adjective} {category} {model}"
        ],
        "desc_templates": [
            "{quality}な{category}です。{feature}機能と{spec}を搭載し、{benefit}。",
            "{brand_name}の{adjective}な{category}です。{feature}対応で{spec}を備えており、{benefit}ための最適な選択です。",
            "{quality}な{spec}を持つ{category}です。{feature}が特徴で、{benefit}。{brand_name}ならではの高品質製品です。"
        ]
    },
    "家電": {
        "name_templates": [
            "{brand_short} {category} {model}",
            "{brand_short} {series} {category}",
            "{brand_short} {adjective} {category}"
        ],
        "desc_templates": [
            "{quality}な設計の{category}です。{feature}機能付きで{benefit}。{spec}でエネルギー効率も抜群です。",
            "{brand_name}の{adjective}な{category}です。{feature}と{spec}を兼ね備え、{benefit}。",
            "高性能な{category}です。{quality}なデザインと{feature}機能を搭載し、{spec}で{benefit}を実現します。"
        ]
    },
    "家具": {
        "name_templates": [
            "{brand_short} {category} {series}",
            "{brand_short} {adjective} {category}",
            "{series} {category} by {brand_short}"
        ],
        "desc_templates": [
            "{quality}な{material}を使用した{category}です。{feature}なデザインで{benefit}。{room}に最適です。",
            "{brand_name}の{adjective}な{category}です。{material}製で{feature}な仕上がり。{benefit}と同時に{room}に調和します。",
            "{quality}で{feature}な{category}です。{material}で作られており、{benefit}。{room}のインテリアを引き立てます。"
        ]
    },
    "衣類": {
        "name_templates": [
            "{brand_short} {adjective} {category}",
            "{brand_short} {category} {series}",
            "{series} {category} - {brand_short}"
        ],
        "desc_templates": [
            "{quality}な{material}を使用した{category}です。{feature}なデザインで{benefit}。{season}におすすめです。",
            "{brand_name}の{adjective}な{category}です。{material}製で{feature}な着心地。{benefit}と同時に{season}に最適です。",
            "{quality}で{feature}な{category}です。{material}で作られており、{benefit}。{season}のファッションを彩ります。"
        ]
    },
    "食品": {
        "name_templates": [
            "{brand_short} {adjective} {category}",
            "{brand_short} {flavor} {category}",
            "{flavor} {category} - {brand_short}"
        ],
        "desc_templates": [
            "{quality}な原材料を使った{category}です。{feature}な味わいで{benefit}。{occasion}におすすめです。",
            "{brand_name}の{adjective}な{category}です。{feature}と{flavor}の風味が特徴で、{benefit}。",
            "{quality}な{flavor}の{category}です。{feature}なレシピで作られており、{benefit}。{occasion}に最適です。"
        ]
    },
    "スポーツ": {
        "name_templates": [
            "{brand_short} {adjective} {category}",
            "{brand_short} {category} {series}",
            "{series} {category} - {brand_short}"
        ],
        "desc_templates": [
            "{quality}な{category}です。{feature}な設計で{benefit}。スポーツパフォーマンスを向上させます。",
            "{brand_name}の{adjective}な{category}です。{feature}機能で{benefit}。",
            "{quality}で{feature}な{category}です。プロ仕様の設計で{benefit}。本格的なスポーツに最適です。"
        ]
    },
    "美容": {
        "name_templates": [
            "{brand_short} {adjective} {category}",
            "{brand_short} {category} {series}",
            "{series} {category} - {brand_short}"
        ],
        "desc_templates": [
            "{quality}な{category}です。{feature}成分で{benefit}。美容と健康をサポートします。",
            "{brand_name}の{adjective}な{category}です。{feature}処方で{benefit}。",
            "{quality}で{feature}な{category}です。厳選された成分で{benefit}。毎日のケアに最適です。"
        ]
    }
}
# 商品説明用の修飾語や特性（日本語文法修正版）
product_attributes = {
    "adjective": [
        "プレミアム", "スタンダード", "コンパクト", "ハイエンド", "エントリー", 
        "プロフェッショナル", "クラシック", "モダン", "スリム", "ポータブル",
        "スマート", "エコ", "ラグジュアリー", "カジュアル", "エレガント"
    ],
    "quality": [
        "高品質", "耐久性に優れた", "軽量", "コンパクト", "洗練された", 
        "革新的", "伝統的", "先進的", "シンプル", "機能的",
        "環境に優しい", "ユーザーフレンドリー", "革命的", "快適", "高性能"
    ],
    "feature": [
        "最新", "革新的", "独自", "特許取得済み", "高度", 
        "ユニーク", "カスタマイズ可能", "統合", "省エネ", "防水",
        "多機能", "自動", "インテリジェント", "直感的", "高性能"
    ],
    "benefit": [
        "快適な使用体験を提供します", "生産性を向上させます", "時間を節約できます", 
        "ストレスを軽減します", "生活の質を高めます", "楽しさを加えます",
        "効率的に作業できます", "健康をサポートします", "創造性を刺激します",
        "リラックスできます", "集中力を高めます", "エネルギーを節約します"
    ],
    "spec": [
        "高解像度ディスプレイ", "長時間バッテリー", "高速プロセッサ", "大容量ストレージ", 
        "高感度センサー", "ノイズキャンセリング機能", "高速充電対応", "生体認証",
        "AIアシスタント", "クラウド連携", "マルチデバイス対応", "拡張性の高い設計"
    ],
    "material": [
        "コットン", "シルク", "ウール", "レザー", "デニム", 
        "ナイロン", "ポリエステル", "リネン", "カシミア", "ベルベット",
        "木材", "金属", "ガラス", "プラスチック", "セラミック"
    ],
    "season": [
        "春", "夏", "秋", "冬", "オールシーズン", 
        "梅雨", "真夏", "初秋", "厳冬", "新学期"
    ],
    "room": [
        "リビング", "ベッドルーム", "キッチン", "オフィス", "ダイニング", 
        "バスルーム", "子供部屋", "書斎", "玄関", "テラス"
    ],
    "flavor": [
        "チョコレート", "バニラ", "ストロベリー", "キャラメル", "抹茶", 
        "レモン", "オレンジ", "メープル", "ココナッツ", "ミント",
        "スパイシー", "ハーブ", "フルーティー", "ナッティ", "クリーミー"
    ],
    "occasion": [
        "パーティー", "日常使い", "贈り物", "特別な日", "休日", 
        "朝食", "ランチ", "ディナー", "軽食", "アウトドア"
    ]
}
# 接続再試行ロジックの実装
def get_connection():
    max_retries = 5
    retry_delay = 2
    
    for attempt in range(max_retries):
        try:
            return mysql.connector.connect(**DB_CONFIG)
        except mysql.connector.Error as err:
            if attempt < max_retries - 1:
                print(f"データベース接続エラー: {err}. 再試行中... ({attempt + 1}/{max_retries})")
                time.sleep(retry_delay)
                retry_delay *= 1.5  # 指数バックオフ
            else:
                print(f"データベース接続に失敗しました: {err}")
                raise
# クエリ実行用の再試行関数
def execute_with_retry(cursor, query, values=None, is_many=False, max_retries=3):
    retry_delay = 1
    
    for attempt in range(max_retries):
        try:
            if is_many:
                if values:
                    return cursor.executemany(query, values)
                return cursor.executemany(query)
            else:
                if values:
                    return cursor.execute(query, values)
                return cursor.execute(query)
        except mysql.connector.Error as err:
            # 接続が切れている場合は再接続
            if not cursor.connection.is_connected():
                print("接続が失われたため再接続しています...")
                conn = get_connection()
                cursor = conn.cursor()
                
            if attempt < max_retries - 1:
                print(f"クエリ実行エラー: {err}. 再試行中... ({attempt + 1}/{max_retries})")
                time.sleep(retry_delay)
                retry_delay *= 1.5  # 指数バックオフ
            else:
                print(f"クエリ実行に失敗しました: {err}")
                raise
# 商品用のモデル番号や型番を生成
def generate_model_number():
    model_types = [
        f"{random.randint(10, 99)}",
        f"{random.choice(string.ascii_uppercase)}{random.randint(10, 999)}",
        f"Mark {random.randint(1, 10)}",
        f"Type-{random.choice(string.ascii_uppercase)}",
        f"Series {random.randint(1, 9)}",
        f"v{random.randint(1, 9)}.{random.randint(0, 9)}"
    ]
    return random.choice(model_types)
# シリーズ名を生成
def generate_series_name():
    prefixes = ["Pro", "Lite", "Max", "Ultra", "Plus", "Mini", "Neo", "Eco", "Smart", ""]
    return random.choice(prefixes)
# 架空のブランド名を生成
def generate_brand():
    prefix = random.choice(brand_prefixes) if random.random() > 0.5 else ""
    root = random.choice(brand_roots)
    suffix = random.choice(brand_suffixes) if random.random() > 0.3 else ""
    
    # ブランド名の組み合わせ方を数パターン用意
    patterns = [
        f"{prefix}{root}",
        f"{root}{suffix}",
        f"{prefix}{root}{suffix}"
    ]
    
    return random.choice(patterns)
# 100個のカテゴリを返す関数
def generate_categories(count=100):
    # 定義済みのカテゴリリストをそのまま返す
    return categories_list.copy()
# 商品名と説明文を生成
def generate_product(brand_name, brand_short, category, category_system):
    # カテゴリシステムを特定（電子機器、家電、家具など）
    for system, cats in category_systems.items():
        if category in cats:
            category_system = system
            break
    
    # カテゴリシステムが特定できない場合はデフォルトで電子機器を使用
    if not category_system:
        category_system = "電子機器"
    
    templates = product_templates[category_system]
    
    # 商品名の生成
    name_template = random.choice(templates["name_templates"])
    model = generate_model_number()
    series = generate_series_name()
    adjective = random.choice(product_attributes["adjective"])
    
    # 必要なすべての属性を準備（flavor を含む）
    flavor = random.choice(product_attributes["flavor"]) if "flavor" in product_attributes else ""
    
    name = name_template.format(
        brand_name=brand_name,
        brand_short=brand_short,
        category=category,
        model=model,
        series=series,
        adjective=adjective,
        flavor=flavor  # flavor パラメータを追加
    )
    
    # 説明文の生成
    desc_template = random.choice(templates["desc_templates"])
    
    # 各カテゴリごとに必要な属性を選択
    attrs = {}
    for attr_key in ["quality", "feature", "benefit", "spec", "material", 
                     "season", "room", "flavor", "occasion", "adjective"]:
        if attr_key in product_attributes:
            attrs[attr_key] = random.choice(product_attributes[attr_key])
    
    # 全ての属性が揃わなくても例外を発生させないようにformat_mapを使用
    description = desc_template.format_map(dict(
        brand_name=brand_name,
        category=category,
        **attrs
    ))
    
    return name, description
# メイン関数：ブランド、カテゴリ、商品データの生成と挿入
def insert_sample_data():
    conn = None
    cursor = None
    
    try:
        print("データベース接続を確立しています...")
        conn = get_connection()
        cursor = conn.cursor()
        
        # --------- ブランドの生成と挿入 ---------
        print("ブランドの生成を開始...")
        brands = []
        brand_shorts = []  # 省略形のブランド名（商品名に使用）
        
        while len(brands) < 100:
            brand = generate_brand()
            # 重複チェック
            if brand not in brands:
                brands.append(brand)
                # 省略形はブランド名の最初の単語
                brand_shorts.append(brand.split()[0] if ' ' in brand else brand)
        
        # ブランドの挿入（小さなバッチに分割）
        batch_size = 20  # 小さめのバッチサイズ
        for i in range(0, len(brands), batch_size):
            batch = brands[i:i+batch_size]
            brand_values = [(brand,) for brand in batch]
            execute_with_retry(cursor, "INSERT INTO brands (name) VALUES (%s)", brand_values, is_many=True)
            conn.commit()
            print(f"ブランドバッチ {i//batch_size + 1}/{(len(brands)-1)//batch_size + 1} を挿入しました。")
        
        print(f"{len(brands)}個のブランドを挿入しました。")
        
        # --------- カテゴリの生成と挿入 ---------
        print("カテゴリの生成を開始...")
        categories = generate_categories(100)
        
        # カテゴリの挿入（小さなバッチに分割）
        for i in range(0, len(categories), batch_size):
            batch = categories[i:i+batch_size]
            category_values = [(category,) for category in batch]
            execute_with_retry(cursor, "INSERT INTO categories (name) VALUES (%s)", category_values, is_many=True)
            conn.commit()
            print(f"カテゴリバッチ {i//batch_size + 1}/{(len(categories)-1)//batch_size + 1} を挿入しました。")
        
        print(f"{len(categories)}個のカテゴリを挿入しました。")
        
        # --------- 商品の生成と挿入 ---------
        print("商品の生成を開始...")
        batch_size = 50  # バッチサイズをさらに小さく設定
        total_records = 1000000
        start_time = time.time()
        last_commit_time = start_time
        commit_interval = 30  # 30秒ごとにコミットを強制
        
        # 各カテゴリのシステム（電子機器、家電など）をマッピング
        category_to_system = {}
        for system, system_categories in category_systems.items():
            for cat in system_categories:
                category_to_system[cat] = system
        
        records_inserted = 0
        
        for i in range(0, total_records, batch_size):
            try:
                # 長時間の処理中に接続が切れていないか確認
                if not conn.is_connected():
                    print("接続が失われたため再接続しています...")
                    conn = get_connection()
                    cursor = conn.cursor()
                values = []
                for j in range(batch_size):
                    if i + j >= total_records:
                        break
                    
                    # ランダムにブランドとカテゴリを選択
                    brand_idx = random.randint(0, 99)
                    category_idx = random.randint(0, 99)
                    
                    brand_name = brands[brand_idx]
                    brand_short = brand_shorts[brand_idx]
                    category = categories[category_idx]
                    
                    # カテゴリシステムの特定
                    category_system = category_to_system.get(category, None)
                    
                    # 商品名と説明文の生成
                    name, description = generate_product(brand_name, brand_short, category, category_system)
                    
                    # brand_idとcategory_idはデータベースのインデックスと一致（1から始まる）
                    values.append((name, description, category_idx + 1, brand_idx + 1))
                
                # バッチ挿入（再試行ロジック使用）
                execute_with_retry(
                    cursor,
                    "INSERT INTO products (name, description, category_id, brand_id) VALUES (%s, %s, %s, %s)",
                    values,
                    is_many=True
                )
                
                # 一定時間ごとにコミットする
                current_time = time.time()
                if current_time - last_commit_time > commit_interval:
                    conn.commit()
                    last_commit_time = current_time
                    print(f"定期コミット実行 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                else:
                    conn.commit()
                
                # 進捗状況更新
                records_inserted = min(i + batch_size, total_records)
                elapsed = time.time() - start_time
                records_per_second = records_inserted / elapsed if elapsed > 0 else 0
                progress = records_inserted / total_records * 100
                
                print(f"挿入済み: {records_inserted}/{total_records} ({progress:.2f}%) - 経過時間: {elapsed:.2f}秒 - 速度: {records_per_second:.2f}レコード/秒")
                
                # 処理を少し遅くして負荷を減らす
                if i % (batch_size * 5) == 0 and i > 0:
                    print("負荷軽減のための短い休止...")
                    time.sleep(1)
                
                # 大量処理の場合、メモリ解放のためのGCを実行
                if i % (batch_size * 5) == 0 and i > 0:
                    import gc
                    gc.collect()
                    
            except mysql.connector.Error as err:
                print(f"エラーが発生しましたが、処理を続行します: {err}")
                # 接続が失われた場合は再接続
                if not conn.is_connected():
                    print("接続が失われたため再接続しています...")
                    conn = get_connection()
                    cursor = conn.cursor()
        
        total_time = time.time() - start_time
        print(f"完了: 合計{records_inserted}件の商品データを挿入しました。 総時間: {total_time:.2f}秒")
    
    except Exception as e:
        print(f"エラーが発生しました: {e}")
        if conn and conn.is_connected():
            try:
                conn.rollback()
                print("トランザクションをロールバックしました。")
            except:
                print("トランザクションのロールバックに失敗しました。")
    finally:
        if cursor:
            try:
                cursor.close()
                print("カーソルを閉じました。")
            except:
                pass
        if conn and conn.is_connected():
            try:
                conn.close()
                print("データベース接続を閉じました。")
            except:
                pass

if __name__ == "__main__":
    try:
        insert_sample_data()
    except KeyboardInterrupt:
        print("\nユーザーによって処理が中断されました。")
    except Exception as e:
        print(f"予期せぬエラーが発生しました: {e}")