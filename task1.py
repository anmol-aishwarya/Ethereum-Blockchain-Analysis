import os
import pandas as pd
import time
import math

timeout = 20
from threading import Timer


# Fastest way to merge 32 files (in my experiments)
def merge_file(data_dir, feature_list=None, df_all=pd.DataFrame()):
    if df_all.empty:
        if feature_list != None:
            dfs = (
                pd.read_csv(os.path.join(data_dir, path), usecols=feature_list) for path in os.listdir(data_dir)
            )
            df = pd.concat(dfs, ignore_index=True)
            return df
        else:
            dfs = (
                pd.read_csv(os.path.join(data_dir, path)) for path in os.listdir(data_dir)
            )
            df = pd.concat(dfs, ignore_index=True)
            return df
    else:
        return df_all


def recursive_merge_and_sort(file_list, feature_list, sort_order, sort_time):
    if len(file_list) == 1:
        df = pd.read_csv(os.path.join(data_dir, file_list[0]), usecols=feature_list)
        start_time = time.time()
        df = df.sort_values(by=sort_order, kind="quicksort", ascending=True)  # increasing order
        end_time = time.time()
        sort_time = end_time - start_time
        return df, sort_time

    num_files = len(file_list)
    split = math.ceil(num_files / 2)
    left_list = file_list[0: split]
    right_list = file_list[split:]
    left_df, sort_time = recursive_merge_and_sort(left_list, feature_list, sort_order, sort_time)
    right_df, sort_time = recursive_merge_and_sort(right_list, feature_list, sort_order, sort_time)
    merge_df = pd.concat([left_df, right_df], ignore_index=True)
    start_time = time.time()
    merge_df = merge_df.sort_values(by=sort_order, kind="merge_sort", ascending=True)  # increasing order
    end_time = time.time()
    sort_time = end_time - start_time
    return merge_df, sort_time


def q1(data_dir, sort_algorithm, df_all=pd.DataFrame()):
    print("Start query!")
    feature_list = ["block_number", "block_time", "gas", "tx_hash"]
    if df_all.empty:
        start_time = time.time()
        df = merge_file(data_dir=data_dir, feature_list=feature_list, df_all=df_all)
        end_time = time.time()
        print(f'Reading file time : {end_time - start_time}s')
    else:
        df = df_all

    start_time = time.time()
    df.drop_duplicates("tx_hash", inplace=True)
    df = df.groupby(["block_number", "block_time"])['gas'].sum().reset_index()
    df["block_gas"] = df["gas"]
    df = df[["block_number", "block_time", "block_gas"]]
    end_time = time.time()
    print(f'Data processing time : {end_time - start_time}s')

    start_time = time.time()
    df = df.sort_values(by=["block_gas"], kind=sort_algorithm, ascending=True)  # increasing order
    end_time = time.time()
    print(f'Sort time : {end_time - start_time}s')
    print("End query!")
    return df


def q2(data_dir, sort_algorithm, df_all=pd.DataFrame()):
    print("Start query!")
    feature_list = ["block_number", "block_time", "tx_hash"]
    if df_all.empty:
        start_time = time.time()
        df = merge_file(data_dir=data_dir, feature_list=feature_list, df_all=df_all)
        end_time = time.time()
        print(f'Reading file time : {end_time - start_time}s')
    else:
        df = df_all
    start_time = time.time()
    df.drop_duplicates("tx_hash", inplace=True)
    df = df.groupby(["block_number", "block_time"])["tx_hash"].size().reset_index()
    df["transactions"] = df["tx_hash"]
    df = df[["block_number", "block_time", "transactions"]]
    end_time = time.time()
    print(f'Data processing time : {end_time - start_time}s')

    start_time = time.time()
    df = df.sort_values(by=["transactions"], kind=sort_algorithm, ascending=True)  # increasing order
    end_time = time.time()
    print(f'Sort time : {end_time - start_time}s')
    print("End query!")
    return df


def q3(data_dir, sort_algorithm, df_all=pd.DataFrame()):
    print("Start query!")
    feature_list = ["tx_hash", "block_number", "block_time", "total_gas", "gas"]

    if df_all.empty:
        start_time = time.time()
        df = merge_file(data_dir=data_dir, feature_list=feature_list, df_all=df_all)
        end_time = time.time()
        print(f'Reading file time : {end_time - start_time}s')
    else:
        df = df_all
    start_time = time.time()
    df.drop_duplicates("tx_hash", inplace=True)
    df["transaction_fee"] = df["total_gas"] - df["gas"]
    df = df.drop(["total_gas", "gas"], axis=1)
    df = df[["tx_hash", "block_number", "block_time", "transaction_fee"]]
    end_time = time.time()
    print(f'Data processing time : {end_time - start_time}s')
    start_time = time.time()
    df = df.sort_values(by=["transaction_fee"], kind=sort_algorithm, ascending=True)  # increasing order
    end_time = time.time()
    print(f'Sort time : {end_time - start_time}s')
    print("End query!")
    return df


def q4(data_dir, sort_algorithm, df_all=pd.DataFrame()):
    print("Start query!")
    feature_list = ["block_number", "block_time", "tx_hash", "gas_price", "gas"]
    if df_all.empty:
        start_time = time.time()
        df = merge_file(data_dir=data_dir, feature_list=feature_list, df_all=df_all)
        end_time = time.time()
        print(f'Reading file time : {end_time - start_time}s')
    else:
        df = df_all
    start_time = time.time()
    df.drop_duplicates("tx_hash", inplace=True)
    df["block_gas"] = df.groupby("block_number", sort=False)["gas"].transform('sum')
    df = df.drop(["gas"], axis=1)
    end_time = time.time()
    print(f'Data processing time : {end_time - start_time}s')
    start_time = time.time()
    df = df.sort_values(by=["block_gas", "gas_price"], kind=sort_algorithm, ascending=True)  # increasing order
    end_time = time.time()
    print(f'Sort time : {end_time - start_time}s')
    df = df.drop(["block_gas"], axis=1)
    df = df[["tx_hash", "gas_price", "block_number", "block_time"]]
    print("End query!")
    return df


def q5(data_dir, sort_algorithm, df_all=pd.DataFrame()):
    print("Start query!")
    feature_list = ["from_addr", "to_addr", "tx_hash", "block_number", "block_time"]
    if df_all.empty:
        start_time = time.time()
        df = merge_file(data_dir=data_dir, feature_list=feature_list, df_all=df_all)
        end_time = time.time()
        print(f'Reading file time : {end_time - start_time}s')
    else:
        df = df_all
    start_time = time.time()
    df = df.sort_values(by=["block_number", "tx_hash"], kind=sort_algorithm, ascending=True)  # increasing order
    df = df[["from_addr", "to_addr", "tx_hash", "block_number", "block_time"]]
    end_time = time.time()
    print(f'Sort time : {end_time - start_time}s')
    print("End query!")
    return df


def q6(data_dir, input_addr, df_all=pd.DataFrame()):
    print("Start query!")
    feature_list = ['tx_hash', 'total_gas', 'gas', 'tx_index_in_block', 'block_number', 'block_time']
    if df_all.empty:
        start_time = time.time()
        df = merge_file(data_dir=data_dir, feature_list=feature_list, df_all=df_all)
        end_time = time.time()
        print(f'Reading file time : {end_time - start_time}s')
    else:
        df = df_all
    start_time = time.time()
    df['transaction_fee'] = df['total_gas'] - df['gas']
    df = df.drop(columns=['total_gas', 'gas'])
    end_time = time.time()
    print(f'Data processing time : {end_time - start_time}s')
    start_time = time.time()
    df = df[df['block_number'] == input_addr]
    end_time = time.time()
    print(f'Search time : {end_time - start_time}s')
    df = df.drop_duplicates()
    df = df.sort_values(by='transaction_fee', ascending=False)
    df = df[['tx_hash', 'transaction_fee', 'tx_index_in_block', 'block_number', 'block_time']]
    print("End query!")
    return df


def q7(data_dir, input_addr, df_all=pd.DataFrame()):
    print("Start query!")
    feature_list = ['tx_hash', 'total_gas', 'gas', 'tx_index_in_block', 'block_number', 'block_time']
    if df_all.empty:
        start_time = time.time()
        df = merge_file(data_dir=data_dir, feature_list=feature_list, df_all=df_all)
        end_time = time.time()
        print(f'Reading file time : {end_time - start_time}s')
    else:
        df = df_all
    start_time = time.time()
    df['transaction_fee'] = df['total_gas'] - df['gas']
    df = df.drop(columns=['total_gas', 'gas'])
    end_time = time.time()
    print(f'Data processing time : {end_time - start_time}s')
    start_time = time.time()
    df = df[df['tx_hash'] == input_addr]
    end_time = time.time()
    print(f'Search time : {end_time - start_time}s')
    df = df.drop_duplicates()
    df = df.sort_values(by='transaction_fee', ascending=False)
    df = df[['tx_hash', 'transaction_fee', 'tx_index_in_block', 'block_number', 'block_time']]
    print("End query!")
    return df


def q8(data_dir, input_addr, df_all=pd.DataFrame()):
    print("Start query!")
    feature_list = ['from_addr', 'tx_hash', 'block_number', 'block_time', 'total_gas', 'gas']
    if df_all.empty:
        start_time = time.time()
        df = merge_file(data_dir=data_dir, feature_list=feature_list, df_all=df_all)
        end_time = time.time()
        print(f'Reading file time : {end_time - start_time}s')
    else:
        df = df_all
    start_time = time.time()
    df['transaction_fee'] = df['total_gas'] - df['gas']
    df = df.drop(columns=['total_gas', 'gas'])
    end_time = time.time()
    print(f'Data processing time : {end_time - start_time}s')
    start_time = time.time()
    df = df[df['from_addr'] == input_addr]

    end_time = time.time()
    print(f'Search time : {end_time - start_time}s')
    df = df.drop_duplicates()
    df = df.sort_values(by='transaction_fee', ascending=False)
    df = df[['from_addr', 'tx_hash', 'block_number', 'block_time', 'transaction_fee']]
    print("End query!")
    return df


def q9(data_dir, input_addr, df_all=pd.DataFrame()):
    print("Start query!")
    feature_list = ['to_addr', 'tx_hash', 'block_number', 'block_time', 'total_gas', 'gas']
    if df_all.empty:
        start_time = time.time()
        df = merge_file(data_dir=data_dir, feature_list=feature_list, df_all=df_all)
        end_time = time.time()
        print(f'Reading file time : {end_time - start_time}s')
    else:
        df = df_all
    start_time = time.time()
    df['transaction_fee'] = df['total_gas'] - df['gas']
    df = df.drop(columns=['total_gas', 'gas'])
    end_time = time.time()
    print(f'Data processing time : {end_time - start_time}s')

    start_time = time.time()
    df = df[df['to_addr'] == input_addr]

    end_time = time.time()
    print(f'Search time : {end_time - start_time}s')
    df = df.drop_duplicates()
    df = df.sort_values(by='transaction_fee', ascending=False)

    df = df[['to_addr', 'tx_hash', 'block_number', 'block_time', 'transaction_fee']]
    print("End query!")
    return df


def q10(data_dir, from_to_bool, input_addr, df_all=pd.DataFrame()):
    print("Start query!")
    if from_to_bool == 0:
        print("from_addr!")
        feature_list = ['from_addr', 'token_qty', 'tx_hash']

        if df_all.empty:
            start_time = time.time()
            df = merge_file(data_dir=data_dir, feature_list=feature_list, df_all=df_all)
            end_time = time.time()
            print(f'Reading file time : {end_time - start_time}s')
        else:
            df = df_all
        start_time = time.time()
        df = df[df['from_addr'] == input_addr]
        # df['token_qty'] = df['token_qty'].astype('float64')

        min_value = min(df['token_qty'].to_numpy().tolist())
        max_value = max(df['token_qty'].to_numpy().tolist())
        df_result = {'from_addr': [input_addr], 'max_token_tranfer': [max_value], 'min_token_transfer': [min_value]}
        df_result = pd.DataFrame(df_result)

        end_time = time.time()
        print(f'Search time : {end_time - start_time}s')

        print('min_value: ', min_value)
        print('max_value: ', max_value)
        print("End query!")
        return df_result
    else:
        print("to_addr!")
        feature_list = ['to_addr', 'token_qty', 'tx_hash']
        if df_all.empty:
            start_time = time.time()
            df = merge_file(data_dir=data_dir, feature_list=feature_list, df_all=df_all)
            end_time = time.time()
            print(f'Reading file time : {end_time - start_time}s')
        else:
            df = df_all
        start_time = time.time()
        df = df[df['to_addr'] == input_addr]
        # df['token_qty'] = df['token_qty'].astype('float64')

        min_value = min(df['token_qty'].to_numpy().tolist())
        max_value = max(df['token_qty'].to_numpy().tolist())
        df_result = {'to_addr': [input_addr], 'max_token_tranfer': [max_value], 'min_token_transfer': [min_value]}
        df_result = pd.DataFrame(df_result)

        end_time = time.time()
        print(f'Search time : {end_time - start_time}s')

        print('min_value: ', min_value)
        print('max_value: ', max_value)
        print("End query!")
        return df_result


def run_query(q_id, data_dir, sort_algorithm, input_addr='', df_all=pd.DataFrame()):
    if q_id == 1:
        res_df = q1(data_dir, sort_algorithm, df_all)
    elif q_id == 2:
        res_df = q2(data_dir, sort_algorithm, df_all)
    elif q_id == 3:
        res_df = q3(data_dir, sort_algorithm, df_all)
    elif q_id == 4:
        res_df = q4(data_dir, sort_algorithm, df_all)
    elif q_id == 5:
        res_df = q5(data_dir, sort_algorithm, df_all)
    elif q_id == 6:
        if input_addr == '':
            input_addr = '11147095'
            print('Block number input value: ', input_addr)
        try:
            input_addr = int(input_addr)
        except:
            print("Input need to be integer!")
            return True
        res_df = q6(data_dir, input_addr, df_all)
    elif q_id == 7:
        if input_addr == '':
            input_addr = '0x153cdd12c67366b1090826f6f248e8fd04a6225923123b41e7d6fa8e315d70f1'
            print('Default input value: ', input_addr)
        res_df = q7(data_dir, input_addr, df_all)
    elif q_id == 8:
        if input_addr == '':
            input_addr = '0x2dfa4eb5bb6ed203057e2fc1438d408ee08b7bc4'
            print('Default input address: ', input_addr)
        res_df = q8(data_dir, input_addr, df_all)
    elif q_id == 9:
        if input_addr == '':
            input_addr = '0x2dfa4eb5bb6ed203057e2fc1438d408ee08b7bc4'
            print('Default input address: ', input_addr)
        res_df = q9(data_dir, input_addr, df_all)
    elif q_id == 10:
        if input_addr == '':
            input_addr = '0x2dfa4eb5bb6ed203057e2fc1438d408ee08b7bc4'
            print('Default input address: ', input_addr)
        res_df = q10(data_dir, 0, input_addr, df_all)
        print('\n', 'Displaying top 10 result (from_addr) for Task# ' + str(q_id) + ':')
        print(pd.DataFrame(res_df).head(10).to_string(index=False), '\n')
        res_df.head(100).to_csv(os.path.join(output_dir, "question_" + str(q_id) + "_output2.csv"), index=False)
        res_df = q10(data_dir, 1, input_addr, df_all)
        print('\n', 'Displaying top 10 result (to_addr) for Task# ' + str(q_id) + ':')
        print(pd.DataFrame(res_df).head(10).to_string(index=False), '\n')
        res_df.head(100).to_csv(os.path.join(output_dir, "question_" + str(q_id) + "_output1.csv"), index=False)
        print('Output saved in /outputs.')
    else:
        print("Wrong question id!")
    # sys.exit(1)
    # res_df.to_csv(os.path.join(output_dir, "question_" + str(args.question_id) + "_output.csv"), index=False)
    # print("Write output done!")
    # print(res_df[:50])
    # print(res_df[50:100])
    if q_id in range(1, 10):
        if res_df.shape[0] == 0:
            print('not found!')
        else:
            print('\n', 'Displaying top 10 result for Task# ' + str(q_id))
            print(pd.DataFrame(res_df).head(10).to_string(index=False), '\n')
            if q_id == 3 or q_id == 6 or q_id == 7 or q_id == 8 or q_id == 9:
                unique_tx_hash = pd.Series(res_df['tx_hash'], name='unique_tx_hash').drop_duplicates(keep='last')
                print('Displaying top 10 unique tx_hash for Task# ' + str(q_id))
                print(pd.DataFrame(unique_tx_hash).head(10).to_string(index=False), '\n')
                unique_tx_hash.head(100).to_csv(
                    os.path.join(output_dir, "question_" + str(q_id) + "_unique_tx_hash.csv"),
                    index=False)
            res_df.head(100).to_csv(os.path.join(output_dir, "question_" + str(q_id) + "_output_100.csv"), index=False)
            print('Output with first 100 records saved in /outputs.')


output_dir = "outputs/"

if not os.path.isdir(output_dir):
    os.mkdir(output_dir)
sort_algorithm = "merge_sort"

# parser = argparse.ArgumentParser()
# parser.add_argument("--question_id", type=int, default=1)
# args = parser.parse_args()

print('Welcome to Group 8 project')
data_dir = input("Please enter transactions data directory or hit Enter to use default data path /CS5413/Ethereum: ")
if data_dir == '':
    data_dir = "/home/scratch1/cs5413/ethereum/transactions"
while not os.path.isdir(data_dir):
    data_dir = input("Directory does not exist - enter other directory:")
    if data_dir == '':
        data_dir = "/home/scratch1/cs5413/ethereum/transactions"
while True:
    input_addr = ''
    q_id = input(
        "Type \'q\' to quit, \'all\' to run all 10 queries with default input values for query 6 to 10, or enter query number: ")
    if q_id == 'q':
        print('Bye Bye')
        break
    if q_id == 'all':
        print('Run all queries with default input values')
        start_time = time.time()
        df_all = merge_file(data_dir=data_dir)
        end_time = time.time()
        print(f"Reading file in: {end_time - start_time}")
        for i in range(1, 11):
            print(f"Question {str(i)}")
            if i in range(6, 11):
                t = Timer(timeout, print, ['Please hit enter to run with default input value'])
                t.start()
                prompt = "Enter input or input with default value will run in %d seconds:\n" % timeout
                input_addr = input(prompt)
                t.cancel()
            start_time = time.time()
            run_query(i, data_dir, sort_algorithm, input_addr, df_all)
            end_time = time.time()
            print(f"Total query running time is: {end_time - start_time}")

    else:
        try:
            q_id = int(q_id)
        except:
            print("Query not found!")
            pass
        if q_id in range(6, 11):
            t = Timer(timeout, print, ['Please hit enter to run with default input value'])
            t.start()
            prompt = "Enter input or input with default value will run in %d seconds:\n" % timeout
            input_addr = input(prompt)
            t.cancel()

        print(f"Question {q_id}")
        start_time = time.time()
        run_query(q_id, data_dir, sort_algorithm, input_addr)
        end_time = time.time()

        print(f"Total query running time is: {end_time - start_time}")
