
import os 
import kagglehub 
import shutil

def get_data_kaggle():
    path = kagglehub.dataset_download("olistbr/brazilian-ecommerce")
    print("Path to dataset files:", path)


    destination = "./dataset"
    if os.path.exists(path) and os.path.exists(destination):
        a = "already moved to destination"
    else:
        shutil.move(path, destination)
        a = f"Success to move to {destination}"
    return print(a)