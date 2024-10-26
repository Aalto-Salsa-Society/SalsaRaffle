#type: ignore

# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "faker",
# ]
# ///

#run this script with uv run datagen.py

from faker import Faker

fake = Faker()  

NUM_MEMBERS = 200
NUM_APPROVED = 170
NUM_REG = 80 #number of people who filled out registration form this cycle


def main(): 
    #main file
    print("this is running datagen")
    print(fake.name())

def gen_membership_file():
    #generate fake data for membership file
    print("hello there")

    
if __name__ == "__main__":
    main()
