import pandas as pd
import random
import multiprocessing


# Generate a large dataset with 1 million rows
def generate_large_dataset():
    num_rows = 1_000_000
    data = {
        'ID': range(1, num_rows + 1),
        'Number': [random.randint(1, 1_000_000) for _ in range(num_rows)]
    }
    df = pd.DataFrame(data)
    df.to_csv("large_numbers.csv", index=False)
    print("Dataset 'large_numbers.csv' generated successfully.")


# Function to process a chunk of data
def process_chunk(chunk, result_dict, index):
    result_dict[index] = {
        'sum': chunk['Number'].sum(),
        'max': chunk['Number'].max(),
        'min': chunk['Number'].min(),
        'even_count': (chunk['Number'] % 2 == 0).sum(),
        'odd_count': len(chunk) - (chunk['Number'] % 2 == 0).sum()
    }


if __name__ == "__main__":
    # Step 1: Generate the dataset
    generate_large_dataset()

    # Step 2: Read the dataset
    df = pd.read_csv("large_numbers.csv")

    # Step 3: Split data into 4 chunks
    chunk_size = 250000  # Each chunk has 250,000 rows
    chunks = [df.iloc[i * chunk_size: (i + 1) * chunk_size] for i in range(4)]

    # Step 4: Use multiprocessing to process chunks
    manager = multiprocessing.Manager()
    result_dict = manager.dict()
    processes = []

    for i in range(4):
        p = multiprocessing.Process(target=process_chunk, args=(chunks[i], result_dict, i))
        processes.append(p)
        p.start()

    for p in processes:
        p.join()

    # Step 5: Aggregate results
    final_sum = sum(result['sum'] for result in result_dict.values())
    final_max = max(result['max'] for result in result_dict.values())
    final_min = min(result['min'] for result in result_dict.values())
    final_even_count = sum(result['even_count'] for result in result_dict.values())
    final_odd_count = sum(result['odd_count'] for result in result_dict.values())

    # Step 6: Print results
    print("Final Results:")
    print(f"Total Sum: {final_sum}")
    print(f"Maximum Number: {final_max}")
    print(f"Minimum Number: {final_min}")
    print(f"Even Count: {final_even_count}")
    print(f"Odd Count: {final_odd_count}")
