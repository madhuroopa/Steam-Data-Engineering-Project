            for message in consumer:
                json_data = message.value.decode('utf-8')  # Convert bytes to string
                print(json_data)
        except Exception as e:
            print("Error in consumer loop:")
        finally:
            consumer.close()

if __name__=="__main__":
    obj=Consumer()
    obj.get_consumer()
    
    