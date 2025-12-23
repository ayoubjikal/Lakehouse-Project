"""
Morocco Census Data Producer
============================
Simulates census data for Morocco and sends to Kafka.
"""

import json
import random
import time
import os
import uuid
from datetime import datetime
from kafka import KafkaProducer

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "census_persons_topic")
SEND_INTERVAL = float(os.getenv("SEND_INTERVAL", "0.1"))

# Morocco regions with codes
REGIONS = {
    "01": "Tanger-Tétouan-Al Hoceïma",
    "02": "L'Oriental",
    "03": "Fès-Meknès",
    "04": "Rabat-Salé-Kénitra",
    "05": "Béni Mellal-Khénifra",
    "06": "Casablanca-Settat",
    "07": "Marrakech-Safi",
    "08": "Drâa-Tafilalet",
    "09": "Souss-Massa",
    "10": "Guelmim-Oued Noun",
    "11": "Laâyoune-Sakia El Hamra",
    "12": "Dakhla-Oued Ed-Dahab"
}

# Cities by region
CITIES_BY_REGION = {
    "01": ["Tanger", "Tétouan", "Al Hoceïma", "Larache", "Chefchaouen"],
    "02": ["Oujda", "Nador", "Berkane", "Taourirt", "Jerada"],
    "03": ["Fès", "Meknès", "Taza", "Sefrou", "Ifrane"],
    "04": ["Rabat", "Salé", "Kénitra", "Témara", "Skhirat"],
    "05": ["Béni Mellal", "Khouribga", "Fquih Ben Salah", "Azilal"],
    "06": ["Casablanca", "Mohammedia", "El Jadida", "Settat", "Berrechid"],
    "07": ["Marrakech", "Safi", "Essaouira", "El Kelâa des Sraghna"],
    "08": ["Errachidia", "Ouarzazate", "Tinghir", "Zagora"],
    "09": ["Agadir", "Inezgane", "Taroudant", "Tiznit"],
    "10": ["Guelmim", "Tan-Tan", "Sidi Ifni", "Assa-Zag"],
    "11": ["Laâyoune", "Boujdour", "Tarfaya"],
    "12": ["Dakhla", "Aousserd"]
}

# Moroccan names
FIRST_NAMES_MALE = [
    "Mohammed", "Ahmed", "Youssef", "Omar", "Ali", "Hassan", "Rachid", 
    "Khalid", "Abdellah", "Mehdi", "Amine", "Karim", "Said", "Hamza",
    "Ayoub", "Bilal", "Ismail", "Zakaria", "Younes", "Othmane"
]

FIRST_NAMES_FEMALE = [
    "Fatima", "Khadija", "Aicha", "Meryem", "Salma", "Nadia", "Houda",
    "Sanaa", "Imane", "Hajar", "Sara", "Zineb", "Amina", "Loubna",
    "Soukaina", "Hanane", "Naima", "Laila", "Samira", "Karima"
]

LAST_NAMES = [
    "Alaoui", "Bennani", "Tazi", "Fassi", "Berrada", "Chaoui", "Idrissi",
    "Lahlou", "Benkirane", "El Amrani", "Benjelloun", "Bouzid", "Chraibi",
    "Dahbi", "El Harti", "Filali", "Guessous", "Hajji", "Ibrahimi", "Jaidi"
]

EDUCATION_LEVELS = ["none", "primary", "secondary", "baccalaureate", "university", "postgraduate"]
EMPLOYMENT_STATUS = ["employed", "unemployed", "student", "retired", "self_employed", "homemaker"]
OCCUPATIONS = [
    "teacher", "engineer", "doctor", "farmer", "merchant", "driver", 
    "artisan", "civil_servant", "technician", "laborer", "manager", "none"
]
HOUSING_TYPES = ["apartment", "house", "villa", "traditional_house", "rural_house"]
MARITAL_STATUS = ["single", "married", "divorced", "widowed"]


def generate_person():
    """Generate a random census person record"""
    
    # Select region and city
    region_code = random.choice(list(REGIONS.keys()))
    region_name = REGIONS[region_code]
    city = random.choice(CITIES_BY_REGION[region_code])
    
    # Generate demographics
    gender = random.choice(["male", "female"])
    if gender == "male":
        first_name = random.choice(FIRST_NAMES_MALE)
    else:
        first_name = random.choice(FIRST_NAMES_FEMALE)
    
    last_name = random.choice(LAST_NAMES)
    
    # Age distribution (realistic for Morocco)
    age_weights = [30, 25, 20, 15, 10]  # More young people
    age_ranges = [(0, 14), (15, 24), (25, 44), (45, 64), (65, 95)]
    age_range = random.choices(age_ranges, weights=age_weights)[0]
    age = random.randint(age_range[0], age_range[1])
    
    # Education based on age
    if age < 6:
        education_level = "none"
    elif age < 12:
        education_level = random.choice(["none", "primary"])
    elif age < 18:
        education_level = random.choice(["primary", "secondary"])
    else:
        education_level = random.choices(
            EDUCATION_LEVELS,
            weights=[10, 20, 25, 20, 20, 5]
        )[0]
    
    # Employment based on age
    if age < 15:
        employment_status = "student" if age >= 6 else "none"
        occupation = "none"
        monthly_income = 0.0
    elif age < 25:
        employment_status = random.choices(
            ["student", "employed", "unemployed"],
            weights=[50, 30, 20]
        )[0]
        occupation = "none" if employment_status != "employed" else random.choice(OCCUPATIONS)
        monthly_income = random.uniform(2000, 5000) if employment_status == "employed" else 0.0
    elif age < 65:
        employment_status = random.choices(
            EMPLOYMENT_STATUS,
            weights=[45, 15, 5, 5, 20, 10]
        )[0]
        if employment_status in ["employed", "self_employed"]:
            occupation = random.choice([o for o in OCCUPATIONS if o != "none"])
            monthly_income = random.uniform(3000, 25000)
        else:
            occupation = "none"
            monthly_income = 0.0
    else:
        employment_status = "retired"
        occupation = "none"
        monthly_income = random.uniform(1500, 8000)
    
    # Marital status based on age
    if age < 18:
        marital_status = "single"
    elif age < 25:
        marital_status = random.choices(
            MARITAL_STATUS,
            weights=[70, 28, 1, 1]
        )[0]
    elif age < 45:
        marital_status = random.choices(
            MARITAL_STATUS,
            weights=[20, 70, 8, 2]
        )[0]
    else:
        marital_status = random.choices(
            MARITAL_STATUS,
            weights=[5, 70, 10, 15]
        )[0]
    
    # Household and housing
    household_size = random.choices(
        [1, 2, 3, 4, 5, 6, 7, 8],
        weights=[5, 10, 15, 25, 20, 15, 7, 3]
    )[0]
    
    housing_type = random.choices(
        HOUSING_TYPES,
        weights=[30, 25, 10, 20, 15]
    )[0]
    
    # Health insurance (higher for employed)
    if employment_status in ["employed", "self_employed", "retired"]:
        has_health_insurance = random.random() < 0.7
    else:
        has_health_insurance = random.random() < 0.3
    
    # Build record - ensure all field names match schema exactly
    person = {
        "person_id": str(uuid.uuid4()),
        "first_name": first_name,
        "last_name": last_name,
        "full_name": f"{first_name} {last_name}",
        "age": age,
        "gender": gender,
        "marital_status": marital_status,
        "region_code": region_code,
        "region_name": region_name,
        "city": city,
        "education_level": education_level,
        "employment_status": employment_status if employment_status != "none" else "unemployed",
        "occupation": occupation,
        "monthly_income": round(monthly_income, 2),  # Ensure it's a float
        "housing_type": housing_type,
        "household_size": household_size,
        "has_health_insurance": has_health_insurance,
        "event_time": datetime.utcnow().isoformat()
    }
    
    return person


def main():
    """Main producer loop"""
    
    print("=" * 60)
    print("🇲🇦 Morocco Census Data Producer")
    print("=" * 60)
    print(f"Kafka Servers: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Topic: {KAFKA_TOPIC}")
    print(f"Send Interval: {SEND_INTERVAL}s")
    print("=" * 60)
    
    # Wait for Kafka to be ready
    print("\n⏳ Waiting for Kafka to be ready...")
    time.sleep(10)
    
    # Create producer
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None
    )
    
    print("✅ Connected to Kafka")
    print("\n🚀 Starting to produce census records...")
    
    records_sent = 0
    start_time = time.time()
    
    try:
        while True:
            # Generate person record
            person = generate_person()
            
            # Send to Kafka (partition by region for ordering)
            producer.send(
                KAFKA_TOPIC,
                key=person["region_code"],
                value=person
            )
            
            records_sent += 1
            
            # Print progress every 100 records
            if records_sent % 100 == 0:
                elapsed = time.time() - start_time
                rate = records_sent / elapsed
                print(f"📤 Sent {records_sent:,} records ({rate:.1f} records/sec)")
            
            # Print sample record every 500 records
            if records_sent % 500 == 0:
                print(f"\n📋 Sample Record:")
                print(f"   Name: {person['full_name']}")
                print(f"   Age: {person['age']} | Gender: {person['gender']}")
                print(f"   City: {person['city']}, {person['region_name']}")
                print(f"   Employment: {person['employment_status']}")
                print(f"   Income: {person['monthly_income']:,.0f} MAD\n")
            
            time.sleep(SEND_INTERVAL)
            
    except KeyboardInterrupt:
        print(f"\n\n🛑 Stopping producer...")
        print(f"📊 Total records sent: {records_sent:,}")
    finally:
        producer.flush()
        producer.close()
        print("✅ Producer stopped")


if __name__ == "__main__":
    main()