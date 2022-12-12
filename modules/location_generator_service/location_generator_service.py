import json
from concurrent import futures

import grpc
import location_pb2
import location_pb2_grpc
from kafka import KafkaProducer

KAFKA_TOPIC = "locations"
KAFKA_PATH = 'my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092'

producer = KafkaProducer(bootstrap_servers=KAFKA_PATH)

class LocationServicer(location_pb2_grpc.LocationServiceServicer):
    def Create(self, request, context):
        location = {
            "person_id": request.person_id,
            "latitude": request.latitude,
            "longitude": request.longitude
        }

        location_encoded = json.dumps(location).encode('utf-8')
        producer.send(KAFKA_TOPIC, location_encoded)
        producer.flush()

        return location_pb2.LocationMessage(**location)


# Initialize gRPC server
server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
location_pb2_grpc.add_LocationServiceServicer_to_server(LocationServicer(), server)

print("Starting gRPC server on port 5005...")
server.add_insecure_port("[::]:5005")
server.start()

# Keep thread alive
server.wait_for_termination()