import { NativeConnection, Worker } from "@temporalio/worker";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const currentDirectory = dirname(fileURLToPath(import.meta.url));
const connection = await NativeConnection.connect({ address: process.env.TEMPORAL_ADDRESS ?? "localhost:7233" });

const worker = await Worker.create({
  connection,
  namespace: process.env.TEMPORAL_NAMESPACE ?? "default",
  taskQueue: process.env.TEMPORAL_TASK_QUEUE ?? "crush-case-review-v1",
  workflowsPath: join(currentDirectory, "caseWorkflow.ts"),
  maxConcurrentWorkflowTaskExecutions: 100,
  maxConcurrentActivityTaskExecutions: 25,
});

await worker.run();
