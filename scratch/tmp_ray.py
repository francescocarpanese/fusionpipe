import ray


ray_port = 6379


ray.init(address=f"localhost:{ray_port}")

@ray.remote
def simple_job(x):
    return x * x

result = ray.get(simple_job.remote(5))
print("Result:", result)