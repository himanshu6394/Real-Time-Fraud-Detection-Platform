
import random
import uuid
from datetime import datetime, timedelta
from typing import Optional
import pandas as pd

PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "upi", "net_banking", "cash", "wallet", "neft", "rtgs"]
TRANSACTION_STATUSES = ["success", "failed", "pending", "reversed", "timeout", "chargeback"]

def generate_synthetic_transactions(num_rows: Optional[int] = None) -> pd.DataFrame:
    """
    Generate a synthetic transaction dataset with realistic fraud patterns, Indian names, and Indian cities.
    Every run is unique and highly randomized. No deterministic seed or caching.
    """
    indian_names = [
        "Ramesh", "Suresh", "Himanshu", "Priya", "Anjali", "Amit", "Sunita", "Vikram", "Neha", "Rahul",
        "Pooja", "Deepak", "Kiran", "Manish", "Sneha", "Arjun", "Meena", "Rohit", "Shreya", "Vikas",
        "Aarav", "Ishaan", "Tanvi", "Kabir", "Aanya", "Lakshmi", "Dev", "Simran", "Yash", "Ritu",
        "Sanjay", "Nisha", "Raj", "Mehul", "Asha", "Gaurav", "Nitin", "Sakshi", "Harsh", "Divya",
        "Kavita", "Siddharth", "Bhavna", "Rajat", "Payal", "Sonal", "Ankit", "Rakesh", "Sheetal", "Tarun",
        "Mona", "Jatin", "Preeti", "Aakash", "Ruchi", "Sandeep", "Tanya", "Vivek", "Aarti", "Kunal",
        "Shyam", "Lata", "Ravi", "Geeta", "Ajay", "Komal", "Ritu", "Ashok", "Rina", "Vijay",
        "Madhuri", "Sanjana", "Prakash", "Rohini", "Sonia", "Anil", "Rohit", "Rina", "Rakesh", "Kishore",
        "Abhishek", "Pankaj", "Ritika", "Shivani", "Rohini", "Sandeep", "Rohit", "Rakesh", "Kishore", "Amitabh",
        "Rajesh", "Ravindra", "Sonal", "Nikita", "Rohit", "Rakesh", "Kishore", "Amit", "Raj", "Ravi"
    ]
    indian_cities = [
        "Mumbai", "Delhi", "Bengaluru", "Kolkata", "Chennai", "Hyderabad", "Pune", "Jaipur", "Lucknow", "Ahmedabad",
        "Chandigarh", "Indore", "Bhopal", "Patna", "Nagpur", "Kanpur", "Surat", "Ranchi", "Guwahati", "Noida",
        "Varanasi", "Amritsar", "Coimbatore", "Vadodara", "Mysuru", "Visakhapatnam", "Thiruvananthapuram", "Agra", "Jodhpur", "Raipur",
        "Udaipur", "Gwalior", "Jamshedpur", "Dehradun", "Shimla", "Panaji", "Srinagar", "Dhanbad", "Faridabad", "Meerut",
        "Aligarh", "Moradabad", "Bareilly", "Allahabad", "Gorakhpur", "Siliguri", "Durgapur", "Asansol", "Bilaspur", "Bhagalpur",
        "Bikaner", "Ajmer", "Haridwar", "Roorkee", "Kota", "Jhansi", "Rewa", "Satna", "Ratlam", "Ujjain"
    ]
    if num_rows is None:
        num_rows = random.randint(46000, 60000)
    now = datetime.now()
    data = []
    for _ in range(num_rows):
        # Realistic fraud rate: ~1% or less
        fraud_type = random.choices(
            ["normal", "card_testing", "device_switch", "location_anomaly", "high_value"],
            [0.985, 0.007, 0.004, 0.003, 0.001]
        )[0]
        user_id = random.choice(indian_names) + "*" + str(uuid.uuid4())[:6]
        merchant_id = "MRC*" + str(uuid.uuid4())[:8]
        device_id = "DEV_" + str(uuid.uuid4())[:8]
        location = random.choice(indian_cities)
        payment_method = random.choice(PAYMENT_METHODS)
        timestamp = now - timedelta(minutes=random.randint(0, 60 * 24 * 90))
        status_weights = [0.85, 0.05, 0.04, 0.03, 0.02, 0.01]
        transaction_status = random.choices(
            TRANSACTION_STATUSES, weights=status_weights
        )[0]
        amount = round(random.uniform(10, 500), 2)
        if fraud_type == "card_testing":
            amount = round(random.uniform(1, 20), 2)
            timestamp = now - timedelta(minutes=random.randint(0, 180))
        elif fraud_type == "device_switch":
            device_id = "DEV_" + str(uuid.uuid4())[:8]
        elif fraud_type == "location_anomaly":
            location = random.choice(indian_cities)
        elif fraud_type == "high_value":
            amount = round(random.uniform(2000, 50000), 2)
        data.append({
            "transaction_id": str(uuid.uuid4()),
            "user_id": user_id,
            "merchant_id": merchant_id,
            "amount": amount,
            "location": location,
            "device_id": device_id,
            "payment_method": payment_method,
            "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            "transaction_status": transaction_status,
        })
    return pd.DataFrame(data)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Generate synthetic transaction data.")
    parser.add_argument("--rows", type=int, default=50000, help="Number of rows to generate.")
    parser.add_argument("--output", type=str, default="../../data/generated/transactions.csv", help="Output CSV path.")
    args = parser.parse_args()
    df = generate_synthetic_transactions(args.rows)
    df.to_csv(args.output, index=False)
    print(f"Generated {args.rows} synthetic transactions at {args.output}")
