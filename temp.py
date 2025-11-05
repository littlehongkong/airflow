import json

def compare_jsonl_stock_lists_by_name(file1_path, file2_path):
    """
    JSONL 파일에서 'name' 필드를 읽어와 차이나는 종목을 비교합니다.

    Args:
        file1_path (str): 첫 번째 JSONL 파일 경로.
        file2_path (str): 두 번째 JSONL 파일 경로.

    Returns:
        tuple: (file1에만 있는 종목 목록, file2에만 있는 종목 목록)
    """
    # 종목 이름이 들어 있는 키를 'name'으로 고정합니다.
    STOCK_NAME_KEY = 'code'

    def load_stocks_from_jsonl(file_path):
        """특정 JSONL 파일에서 종목 이름(name) 목록을 로드하는 내부 함수"""
        stocks = set()
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if not line:
                        continue

                    try:
                        data = json.loads(line)
                        stock_name = data.get(STOCK_NAME_KEY) # .get()을 사용하여 키가 없어도 에러 발생 방지

                        if stock_name:
                            stocks.add(stock_name.strip())
                        else:
                            # 'name' 키가 없는 경우 경고 메시지 출력
                            print(f"⚠️ 경고: {file_path} 파일의 {line_num}번째 줄에 키 '{STOCK_NAME_KEY}' 값이 없습니다.")
                    except json.JSONDecodeError:
                        print(f"❌ 오류: {file_path} 파일의 {line_num}번째 줄이 유효한 JSON 형식이 아닙니다.")
                        continue

            return stocks
        except FileNotFoundError:
            print(f"❌ 오류: 파일 경로를 찾을 수 없습니다 - {file_path}")
            return set()
        except Exception as e:
            print(f"❌ 파일 읽기 중 예상치 못한 오류 발생: {e}")
            return set()

    # 두 파일에서 종목 목록 로드
    stocks1 = load_stocks_from_jsonl(file1_path)
    stocks2 = load_stocks_from_jsonl(file2_path)

    # set 자료형의 차집합 연산을 사용하여 차이나는 종목 식별
    only_in_file1 = stocks1 - stocks2
    only_in_file2 = stocks2 - stocks1

    print(f"\n--- 파일 1 ({file1_path}) 정보 ---")
    print(f"총 종목 수: {len(stocks1)}")
    print(f"--- 파일 2 ({file2_path}) 정보 ---")
    print(f"총 종목 수: {len(stocks2)}")
    print("-" * 30)

    return only_in_file1, only_in_file2

# 사용 예시:
file_a = '2025-10-21_equity_prices.jsonl'
file_b = '2025-10-22_equity_prices.jsonl'

# 테스트를 위한 가상의 JSONL 데이터 (실제 파일은 이 형식을 따릅니다)


# 함수 실행
only_a, only_b = compare_jsonl_stock_lists_by_name(file_a, file_b)

# 결과 출력
print("\n=== 종목 비교 결과 ===")
if only_a or only_b:
    if only_a:
        print(f"✅ 파일 A에만 있는 종목 (B에는 없음): {sorted(list(only_a))}")
    if only_b:
        print(f"✅ 파일 B에만 있는 종목 (A에는 없음): {sorted(list(only_b))}")
else:
    print("두 파일의 종목 목록이 완전히 일치합니다.")