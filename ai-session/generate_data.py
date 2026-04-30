import csv
import random
from datetime import datetime, timedelta

# Set seed for reproducibility
random.seed(42)

# Current date
now = datetime(2026, 4, 29)

# Number of renters
num_renters = 1000

# Number of special renters that meet all conditions
num_special = 100

# Generate renter_ids
renter_ids = [f'renter_{i}' for i in range(num_renters)]

# Event types
event_types = ['page_view', 'search', 'application_start', 'application_complete', 'lease_signed']

# Channels
channels = ['web', 'ios', 'android']

# UTM sources
utm_sources = ['google', 'facebook', 'email', 'direct', None]

# Subscription statuses
sub_statuses = ['active', 'churned', 'never_subscribed']

# Suppression reasons
sup_reasons = ['unsubscribed', 'bounced', 'complained', 'dnc_registry']

# Generate profiles
profiles = []
suppressions = set()  # set of renter_ids in suppression

for i, renter_id in enumerate(renter_ids):
    is_special = i < num_special
    if is_special:
        # Force meet conditions
        last_login = now - timedelta(days=random.randint(31, 365))
        sub_status = 'churned'
        phone = f'+1{random.randint(1000000000, 9999999999)}'
        sms_consent = True
        email_consent = random.choice([True, False])
        dnd_until = None
        created_at = now - timedelta(days=random.randint(100, 1000))
        # Not in suppression
    else:
        last_login = now - timedelta(days=random.randint(1, 365))
        sub_status = random.choice(sub_statuses)
        phone = f'+1{random.randint(1000000000, 9999999999)}' if random.random() > 0.1 else None
        sms_consent = random.choice([True, False])
        email_consent = random.choice([True, False])
        dnd_until = None if random.random() > 0.5 else now + timedelta(days=random.randint(-30, 30))
        created_at = now - timedelta(days=random.randint(100, 1000))
        # Some in suppression
        if random.random() < 0.1:
            suppressions.add(renter_id)

    email = f'{renter_id}@example.com'
    profiles.append({
        'renter_id': renter_id,
        'email': email,
        'phone': phone,
        'last_login': last_login.isoformat(),
        'subscription_status': sub_status,
        'sms_consent': sms_consent,
        'email_consent': email_consent,
        'dnd_until': dnd_until.isoformat() if dnd_until else None,
        'created_at': created_at.isoformat()
    })

# Generate suppression list
suppression_data = []
for renter_id in suppressions:
    suppressed_at = now - timedelta(days=random.randint(1, 365))
    reason = random.choice(sup_reasons)
    suppression_data.append({
        'renter_id': renter_id,
        'suppression_reason': reason,
        'suppressed_at': suppressed_at.isoformat()
    })

# Generate activities
activities = []
for renter_id in renter_ids:
    num_events = random.randint(1, 20)
    for _ in range(num_events):
        event_type = random.choice(event_types)
        # For special renters, ensure at least 3 searches in past 90 days
        if renter_id in [f'renter_{i}' for i in range(num_special)] and event_type != 'search':
            # Force some searches
            if random.random() < 0.3:
                event_type = 'search'
        event_timestamp = now - timedelta(days=random.randint(1, 365))
        property_id = f'prop_{random.randint(1, 1000)}'
        channel = random.choice(channels)
        utm = random.choice(utm_sources)
        activities.append({
            'renter_id': renter_id,
            'event_type': event_type,
            'event_timestamp': event_timestamp.isoformat(),
            'property_id': property_id,
            'channel': channel,
            'utm_source': utm
        })

# For special renters, add extra searches if needed
for i in range(num_special):
    renter_id = f'renter_{i}'
    existing_searches = [a for a in activities if a['renter_id'] == renter_id and a['event_type'] == 'search' and (now - datetime.fromisoformat(a['event_timestamp'])).days <= 90]
    while len(existing_searches) < 3:
        event_timestamp = now - timedelta(days=random.randint(1, 89))
        property_id = f'prop_{random.randint(1, 1000)}'
        channel = random.choice(channels)
        utm = random.choice(utm_sources)
        activities.append({
            'renter_id': renter_id,
            'event_type': 'search',
            'event_timestamp': event_timestamp.isoformat(),
            'property_id': property_id,
            'channel': channel,
            'utm_source': utm
        })
        existing_searches.append(1)  # dummy

# Write to CSV
with open('renter_profiles.csv', 'w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=['renter_id', 'email', 'phone', 'last_login', 'subscription_status', 'sms_consent', 'email_consent', 'dnd_until', 'created_at'])
    writer.writeheader()
    writer.writerows(profiles)

with open('suppression_list.csv', 'w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=['renter_id', 'suppression_reason', 'suppressed_at'])
    writer.writeheader()
    writer.writerows(suppression_data)

with open('renter_activity.csv', 'w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=['renter_id', 'event_type', 'event_timestamp', 'property_id', 'channel', 'utm_source'])
    writer.writeheader()
    writer.writerows(activities)

print("Data generated successfully.")
