def mount_bucket():
  if not any(mount.mountPoint == f"/mnt/{bucket}" for mount in dbutils.fs.mounts()):
    print('Mount bucket!')
    dbutils.fs.mount(f"s3a://{bucket}", f"/mnt/{bucket_2}")

def unmount_bucket():
  if any(mount.mountPoint ==f"/mnt/{bucket}" for mount in dbutils.fs.mounts()):
    print('Unmount bucket!')
    dbutils.fs.unmount(f"/mnt/{bucket}")
