Backend to drive the pipeline system. 

The pipeline are stored in a database.
The backend provides an API to manage pipelines, including creating, updating, and deleting them.

This way the frontend can interact with the backend to manage pipelines.



For the moment
- Unique .env file for all the configuration of the API.
- At test time, I generate a tempoary on disk db.
- The API is initialise having this reference.
- Test are done by writing to the db and check that the API call do their job