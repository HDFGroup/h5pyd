import time
import h5pyd

domain_path = "/home/test_user1/test/one_group.h5"

f = h5pyd.File(domain_path, 'w')

time_stamp = f.last_scan  # get the last scan time
f.create_group("g1")  # create a new group
ts = time.time()
print("waiting for scan update")
while f.last_scan == time_stamp:
    time.sleep(0.1)  # wait for summary data to be updated
wait_time = time.time() - ts
print(f"last_scan updated after: {wait_time:6.2f} seconds")
# print affected summary properties
print("num_groups:", f.num_groups)
print("num_objects:", f.num_objects)
print("metadata_bytes:", f.metadata_bytes)
print("total_size:", f.total_size)
