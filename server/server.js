import express from "express";
import cors from "cors";
import { spawn } from "child_process";
import path from "path";
import { fileURLToPath } from "url";
import PocketBase from "pocketbase";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const PORT = process.env.PORT || 4000;
const PB_URL = process.env.POCKETBASE_URL || "http://127.0.0.1:8092";
const PB_EMAIL = process.env.PB_EMAIL || "admin@test.com";
const PB_PASSWORD = process.env.PB_PASSWORD || "admin@123";

const pb = new PocketBase(PB_URL);
pb.autoCancellation(false);

app.use(cors());
app.use(express.json());

// Track running processes by runId: { pid, child, groupUrl }
const runningProcesses = new Map();

// Authenticate to PocketBase
async function ensureAuth() {
  if (!pb.authStore.isValid) {
    try {
      await pb.collection("_superusers").authWithPassword(PB_EMAIL, PB_PASSWORD);
      console.log("[Server] Superuser authenticated to PocketBase 8092");
    } catch (err) {
      console.error("[Server] PB auth error:", err.message);
    }
  }
}

// Stats endpoint
app.get("/api/stats", async (req, res) => {
  try {
    await ensureAuth();
    const runsResult = await pb.collection("scraping_runs").getList(1, 100, { sort: "-start_time" });
    const postsCount = await pb.collection("posts").getList(1, 1);

    const runs = runsResult.items || [];
    const totalRuns = runsResult.totalItems || runs.length;
    const successfulRuns = runs.filter((r) => r.status === "completed").length;
    const failedRuns = runs.filter((r) => r.status === "failed").length;
    const activeRuns = runs.filter((r) => r.status === "running").length;

    res.json({
      totalRuns,
      successfulRuns,
      failedRuns,
      activeRuns,
      totalPosts: postsCount.totalItems || 0,
      recentRuns: runs.slice(0, 5),
    });
  } catch (err) {
    console.error("[Stats Error]", err);
    res.status(500).json({ error: err.message, details: err.data || err });
  }
});

// List runs
app.get("/api/runs", async (req, res) => {
  try {
    await ensureAuth();
    const page = parseInt(req.query.page) || 1;
    const perPage = parseInt(req.query.perPage) || 20;
    const result = await pb.collection("scraping_runs").getList(page, perPage, {
      sort: "-start_time",
    });
    res.json(result);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Get single run
app.get("/api/runs/:id", async (req, res) => {
  try {
    await ensureAuth();
    const run = await pb.collection("scraping_runs").getOne(req.params.id);
    const logs = await pb.collection("scraping_run_logs").getFullList({
      filter: `run_id = "${req.params.id}"`,
      sort: "timestamp",
    });
    res.json({ run, logs });
  } catch (err) {
    res.status(404).json({ error: "Run not found" });
  }
});

// Start new scraping run
app.post("/api/runs", async (req, res) => {
  const { groupUrl, postLimit } = req.body;

  if (!groupUrl || !groupUrl.includes("facebook.com")) {
    return res.status(400).json({ error: "A valid Facebook group URL is required." });
  }

  const validLimits = [10, 20, 50, 100];
  const limit = parseInt(postLimit) || 10;
  if (!validLimits.includes(limit)) {
    return res.status(400).json({ error: "Post limit must be 10, 20, 50, or 100." });
  }

  try {
    await ensureAuth();

    // Create run record in PocketBase
    const record = await pb.collection("scraping_runs").create({
      actor_name: "Facebook Groups Scraper",
      group_url: groupUrl.trim(),
      post_limit: limit,
      scraped_count: 0,
      status: "pending",
      start_time: new Date().toISOString(),
    });

    const runId = record.id;
    const pythonExe = "D:\\fb group scrpe\\venv\\Scripts\\python.exe";
    const workerScript = path.resolve(__dirname, "../engine/run_worker.py");

    console.log(`[Worker] Spawning scraper for run ${runId}`);
    const child = spawn(
      pythonExe,
      [
        workerScript,
        "--group-url",
        groupUrl.trim(),
        "--post-limit",
        String(limit),
        "--run-id",
        runId,
      ],
      {
        cwd: path.resolve(__dirname, "../engine"),
        env: {
          ...process.env,
          POCKETBASE_URL: PB_URL,
          PB_EMAIL,
          PB_PASSWORD,
        },
      }
    );

    runningProcesses.set(runId, { child, pid: child.pid });

    // Update with PID
    await pb.collection("scraping_runs").update(runId, {
      pid: child.pid,
      status: "running",
    });

    child.stdout.on("data", (data) => {
      console.log(`[Run ${runId}] ${data.toString().trim()}`);
    });

    child.stderr.on("data", (data) => {
      console.error(`[Run ${runId} ERR] ${data.toString().trim()}`);
    });

    child.on("close", async (code) => {
      console.log(`[Run ${runId}] Process exited with code ${code}`);
      runningProcesses.delete(runId);
      try {
        await ensureAuth();
        const cur = await pb.collection("scraping_runs").getOne(runId);
        if (cur.status === "running") {
          await pb.collection("scraping_runs").update(runId, {
            status: code === 0 ? "completed" : "failed",
            end_time: new Date().toISOString(),
          });
        }
      } catch (e) {
        console.error("Error updating run on close:", e.message);
      }
    });

    res.status(201).json({
      message: "Scraping run started successfully",
      runId,
      status: "running",
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Cancel active run
app.post("/api/runs/:id/cancel", async (req, res) => {
  const runId = req.params.id;
  try {
    await ensureAuth();
    const run = await pb.collection("scraping_runs").getOne(runId);

    const proc = runningProcesses.get(runId);
    if (proc && proc.child) {
      try {
        spawn("taskkill", ["/pid", String(proc.child.pid), "/f", "/t"]);
      } catch (e) {
        proc.child.kill("SIGKILL");
      }
      runningProcesses.delete(runId);
    } else if (run.pid) {
      try {
        spawn("taskkill", ["/pid", String(run.pid), "/f", "/t"]);
      } catch (e) {}
    }

    await pb.collection("scraping_runs").update(runId, {
      status: "cancelled",
      end_time: new Date().toISOString(),
    });

    await pb.collection("scraping_run_logs").create({
      run_id: runId,
      level: "WARN",
      message: "Scraping run cancelled by user.",
      timestamp: new Date().toISOString(),
    });

    res.json({ message: "Run cancelled successfully", runId });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Retry run
app.post("/api/runs/:id/retry", async (req, res) => {
  const runId = req.params.id;
  try {
    await ensureAuth();
    const originalRun = await pb.collection("scraping_runs").getOne(runId);

    // Call start run internally
    const newRecord = await pb.collection("scraping_runs").create({
      actor_name: originalRun.actor_name || "Facebook Groups Scraper",
      group_url: originalRun.group_url,
      post_limit: originalRun.post_limit,
      scraped_count: 0,
      status: "pending",
      start_time: new Date().toISOString(),
    });

    const newRunId = newRecord.id;
    const pythonExe = "D:\\fb group scrpe\\venv\\Scripts\\python.exe";
    const workerScript = path.resolve(__dirname, "../engine/run_worker.py");

    const child = spawn(
      pythonExe,
      [
        workerScript,
        "--group-url",
        originalRun.group_url,
        "--post-limit",
        String(originalRun.post_limit),
        "--run-id",
        newRunId,
      ],
      {
        cwd: path.resolve(__dirname, "../engine"),
        env: {
          ...process.env,
          POCKETBASE_URL: PB_URL,
          PB_EMAIL,
          PB_PASSWORD,
        },
      }
    );

    runningProcesses.set(newRunId, { child, pid: child.pid });

    await pb.collection("scraping_runs").update(newRunId, {
      pid: child.pid,
      status: "running",
    });

    res.json({
      message: "Retry run launched",
      newRunId,
      status: "running",
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// SSE Live logs stream
app.get("/api/runs/:id/logs/stream", async (req, res) => {
  const runId = req.params.id;
  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");
  res.flushHeaders();

  let lastLogTime = new Date(0).toISOString();
  let interval = setInterval(async () => {
    try {
      await ensureAuth();
      const logs = await pb.collection("scraping_run_logs").getFullList({
        filter: `run_id = "${runId}" && created > "${lastLogTime}"`,
        sort: "created",
      });

      if (logs.length > 0) {
        lastLogTime = logs[logs.length - 1].created;
        res.write(`data: ${JSON.stringify(logs)}\n\n`);
      }

      // Check run status
      const run = await pb.collection("scraping_runs").getOne(runId);
      if (run.status === "completed" || run.status === "failed" || run.status === "cancelled") {
        res.write(`event: done\ndata: ${JSON.stringify({ status: run.status })}\n\n`);
        clearInterval(interval);
        res.end();
      }
    } catch (e) {
      // client disconnected
      clearInterval(interval);
      res.end();
    }
  }, 1000);

  req.on("close", () => {
    clearInterval(interval);
  });
});

// Datasets: list posts with filters
app.get("/api/datasets", async (req, res) => {
  try {
    await ensureAuth();
    const page = parseInt(req.query.page) || 1;
    const perPage = parseInt(req.query.perPage) || 20;
    const runId = req.query.runId;
    const search = req.query.search;

    let filter = "";
    if (runId) {
      filter += `run_id = "${runId}"`;
    }
    if (search) {
      const searchFilter = `(body ~ "${search}" || author ~ "${search}" || group_name ~ "${search}")`;
      filter = filter ? `${filter} && ${searchFilter}` : searchFilter;
    }

    const posts = await pb.collection("posts").getList(page, perPage, {
      filter,
      sort: "-scraped_at",
    });

    res.json(posts);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Export dataset CSV or JSON
app.get("/api/datasets/export", async (req, res) => {
  try {
    await ensureAuth();
    const format = req.query.format || "json";
    const runId = req.query.runId;

    let filter = runId ? `run_id = "${runId}"` : "";
    const items = await pb.collection("posts").getFullList({
      filter,
      sort: "-scraped_at",
    });

    if (format === "csv") {
      res.setHeader("Content-Type", "text/csv");
      res.setHeader("Content-Disposition", `attachment; filename=facebook_posts_${Date.now()}.csv`);

      const headers = [
        "Facebook Post ID",
        "Author",
        "Description",
        "Posted Date",
        "Post URL",
        "Image URLs",
        "Group Name",
        "Scraped At",
      ];
      let csv = headers.join(",") + "\n";

      for (const item of items) {
        const row = [
          `"${(item.facebook_post_id || "").replace(/"/g, '""')}"`,
          `"${(item.author || "").replace(/"/g, '""')}"`,
          `"${(item.body || "").replace(/"/g, '""').replace(/\n/g, " ")}"`,
          `"${(item.posted_at || "").replace(/"/g, '""')}"`,
          `"${(item.post_url || "").replace(/"/g, '""')}"`,
          `"${(JSON.stringify(item.image_urls || [])).replace(/"/g, '""')}"`,
          `"${(item.group_name || "").replace(/"/g, '""')}"`,
          `"${(item.scraped_at || "").replace(/"/g, '""')}"`,
        ];
        csv += row.join(",") + "\n";
      }
      return res.send(csv);
    }

    // Default JSON
    res.setHeader("Content-Type", "application/json");
    res.setHeader("Content-Disposition", `attachment; filename=facebook_posts_${Date.now()}.json`);
    res.json(items);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.listen(PORT, () => {
  console.log(`[API Server] Running at http://127.0.0.1:${PORT}`);
  console.log(`[API Server] Target PocketBase: ${PB_URL}`);
});
