from pyflink.datastream import StreamExecutionEnvironment

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    data_stream = env.from_collection([1, 2, 3, 4, 5])

    mapped_stream = data_stream.map(lambda x: x * 2)

    mapped_stream.print()

    env.execute("Flink Hello World")

if __name__ == "__main__":
    main()