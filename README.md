_Project Passed_


# Running the Simulation
There are two pieces to the simulation, the `producer` and `consumer`. As you develop each piece of the code, it is recommended that you only run one piece of the project at a time.

However, when you are ready to verify the end-to-end system prior to submission, it is critical that you open a terminal window for each piece and run them at the same time. If you do not run both the `producer` and `consumer` at the same time you will not be able to successfully complete the project.

To run the `producer`:

`cd producers`

`python simulation.py`

To run the Faust Stream Processing Application:

`cd consumers`

`faust -A faust_stream worker -l info`

To run the KSQL Creation Script:

`cd consumers`

`python ksql.py`

To run the `consumer`:

`cd consumers`

`python server.py`

Once the server is running, you may hit `Ctrl+C` at any time to exit.