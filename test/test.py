import sys

sys.path.append('../src/')
from grpcbigbuffer import compile_pb2

# Demostración de mensajes vacíos con longitud mayor a 0 (índice y longitud 0 del mensaje (\x00))

metadata = compile_pb2.celaut__pb2.Any.Metadata()

print('metadata -> ', metadata.SerializeToString())


service = compile_pb2.Service()
service.container.entrypoint.extend(['start.sh'])
print('service -> ', service.SerializeToString())

service_with_meta = compile_pb2.ServiceWithMeta(
    metadata=metadata,
    service=service
)

print('service with meta -> ', service_with_meta.ByteSize(), service_with_meta.SerializeToString())

service_with_meta.Clear()

print('service with meta -> ', service_with_meta.ByteSize(), service_with_meta.SerializeToString())