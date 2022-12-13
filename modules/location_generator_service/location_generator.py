import grpc
import location_pb2
import location_pb2_grpc

"""
Sample implementation of a location generator that can be used to generate location messages to gRPC.
"""

print("Sending sample payload...")

channel = grpc.insecure_channel("localhost:30007")
stub = location_pb2_grpc.LocationServiceStub(channel)

# Update this with desired payload
locations_payload = [{"person_id": 1, "latitude": "-122.3", "longitude": "37.5"}, {"person_id": 5, "latitude": "35.0", "longitude": "-106.5"}]

locations = [location_pb2.LocationMessage(person_id=x['person_id'], latitude=x['latitude'], longitude=x['longitude']) for x in locations_payload]

for location in locations:
    response = stub.Create(location)
    print(f"Response from gRPC server: {response}")
