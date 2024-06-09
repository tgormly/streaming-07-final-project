'''
Tim Gormly 
6/9/2024

Data Streaming Final Project
----------------------------
This script will create a CSV of records that are meant to simulate new adotoption listing from animal shelters around the United States.  
The file generated will be used as a data source to simulate streaming data.
'''



from faker import Faker
import random
from datetime import datetime, timedelta
import csv

# Initialize the Faker library
fake = Faker()

# Define lists of random attributes
dog_breeds = [
    'Labrador Retriever', 'German Shepherd', 'Golden Retriever', 'Bulldog', 'Poodle',
    'Beagle', 'Rottweiler', 'Yorkshire Terrier', 'Boxer', 'Dachshund'
]
cat_breeds = [
    'Siamese', 'Persian', 'Maine Coon', 'Ragdoll', 'Sphynx',
    'British Shorthair', 'Abyssinian', 'Birman', 'Oriental Shorthair', 'Scottish Fold'
]
colors = [
    'Black', 'White', 'Brown', 'Gray', 'Golden', 'Spotted', 'Striped', 'Cream', 'Blue', 'Red'
]
animal_types = ['Dog', 'Cat']
animal_shelters = [
    'Happy Paws Rescue', 'Furry Friends Haven', 'Whiskers and Tails Sanctuary',
    'Paws and Claws Shelter', 'Safe Haven Animal Shelter', 'Four-Legged Friends Rescue',
    'Barks and Meows Haven', 'Loving Paws Rescue', 'Second Chance Animal Shelter',
    'Furever Friends Sanctuary', 'Wagging Tails Shelter', 'Animal Haven Rescue',
    'Rescue Me Animal Shelter', 'Fur-Ever Homes Shelter', 'Pet Haven Rescue',
    'Hopeful Hearts Animal Shelter', 'Cuddle Buddies Rescue', 'Faithful Friends Animal Shelter',
    'Pawsitive Outcomes Rescue', 'Furry Companions Shelter'
]

def generate_random_date_within_last_month():
    '''This function returns a random datetime within the last 30 days.
    This return is a datetime object'''
    today = datetime.today()
    start_date = today - timedelta(days=30)

    random_date = start_date + timedelta(days=random.randint(0, 30),
                                         hours=random.randint(0, 23),
                                         minutes=random.randint(0, 59),
                                         seconds=random.randint(0, 59))
    return random_date

def generate_fake_animal_data(num_records):
    '''Generates and returns a list of fake animal adoption listings
    
    num_records (int) - determines the number of records created'''
    data = []

    #primary loop that creates one record using random and Faker
    for _ in range(num_records):
        animal_type = random.choice(animal_types)
        breed = random.choice(dog_breeds) if animal_type == 'Dog' else random.choice(cat_breeds)
        name = fake.first_name()
        age = random.randint(1, 15)  # Random age between 1 and 15 years
        color = random.choice(colors)
        shelter_name = random.choice(animal_shelters)
        shelter_city = fake.city()
        shelter_state = fake.state()
        adoption_date = generate_random_date_within_last_month()
        
        record = {
            'Name': name,
            'Type': animal_type,
            'Breed': breed,
            'Age': age,
            'Color': color,
            'Shelter Name': shelter_name,
            'Shelter City': shelter_city,
            'Shelter State': shelter_state,
            'Adoption Date': adoption_date
        }
        data.append(record)
    
    # Sort so earliest rows are first
    data.sort(key=lambda x: x['Adoption Date'])
    
    return data

# Generate 2,500 fake animal adoption listings
fake_animal_data = generate_fake_animal_data(2500)

# Write data to CSV file
with open('adoption_data.csv', 'w', newline='') as csvfile:
    fieldnames = ['Name', 'Type', 'Breed', 'Age', 'Color', 'Shelter Name', 'Shelter City', 'Shelter State', 'Adoption Date']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    
    writer.writeheader()
    for entry in fake_animal_data:
        # Format the date to the desired string format before writing to CSV
        entry['Adoption Date'] = entry['Adoption Date'].strftime('%m/%d/%y %H:%M:%S')
        writer.writerow(entry)

# # Print the generated data
# for entry in fake_animal_data:
#     print(entry)
