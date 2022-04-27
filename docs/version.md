# Version History

## v0.6.10

- Check system arguments.
- Allow GPU selection in resources.

## v0.6.9

- Handle exception from GPUtil.

## v0.6.8

- Update docs and requirements.

## v0.6.7

- Fix utils mp backend.

## v0.6.6

- Optimize process_map util.

## v0.6.5

- Fix repr.

## v0.6.4

- Use cpu affinity in resource scan.

## v0.6.3

- Add process_map util.

## v0.6.2

- Simplity logging content.
- Resource required for each stage.

## v0.6.1

- More robust about raising ulimit.

## v0.6.0

- Add result class and seperate result collection and merge logics.

## v0.5.2

- Reduce warnings of reorder buffer size.

## v0.5.1

- More robust handling of pipeline death.

## v0.5.0

- Fix pipeline restarting.
- Larger default timeouts controled by start method.
- Job retry logic moved to wait_job.
- Prevent resetting a pipeline multiple times.
- Ensure job queue and result queue empty when ending.

## v0.4.5

- More robust retry of jobs.

## v0.4.4

- Remove close process to avoid bug and set longer terminate timeout.
- Larger pipeline queue size and longer task and job timeouts.

## v0.4.3

- Concurrency optimization for system.
- Add add_jobs and wait_job methods.

## v0.4.2

- Smooth system ending when some pipelines terminate early.
- Fix repr of ControlTask.

## v0.4.1

- Fix compatibility of SyncPipeline.

## v0.4.0

- Unified use of multiprocessing manager.
- Timeout option for all blocking operations in the main process.
- Use SimpleQueue for system job and result management.
- Retry failed job and rebuild its pipeline.
- Added system.wait_jobs interface.
- Precisely ignore monitor exceptions during system termination.
- Refined type annotation.

## v0.3.9

- Redirect faulthandler log file.
- Clean up imports.

## v0.3.8

- Fix log file option env.

## v0.3.7

- Add log file option.

## v0.3.6

- Pass exceptions through pipeline monitor.

## v0.3.5

- Bypass through reorder stage.
- Robust task fail.

## v0.3.4

- Unified queue exceptions.

## v0.3.3

- Bypass failed tasks.
- Avoid hanging at termination.

## v0.3.2

- System termination.

## v0.3.1

- Parallel initialization for multiple pipelines.

## v0.3.0

- Retry if Pytorch exhausts system's shared memory.
- Wait for AsyncPipeline initialization.
- Better handling at KeyboardInterrupt.

## v0.2.10

- Custom arguments for system.

## v0.2.9

- Fix pipeline wait with multiple workers in the last stage.
- Ignore none return from process function.

## v0.2.8

- Custom arguments for stages.

## v0.2.7

- Fix option.

## v0.2.6

- Option management.

## v0.2.5

- Thread safe progress bar, and silent option.

## v0.2.4

- Silent progress bar.

## v0.2.3

- Fault handler.
- Fix reorder stage.
- Set number of pipelines.
- Allocate resource for workers.

## v0.2.2

- Debug mode control.

## v0.2.1

- Fix reorder stage and sync pipeline.
- Debug modes and task logs.
- More flexible resource allocation.

## v0.1.2

- Initial release.
