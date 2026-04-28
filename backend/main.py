import io
import paramiko
from fastapi import FastAPI, UploadFile, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

SABALCORE_HOST = "login.sabalcore.com"
SABALCORE_PORT = 22
REMOTE_DIR = "~/bfr"
STARCCM_MODULE = "starccm/20.04.007"
POD_KEY = "F7/LkUXpj2wKzca9mMuVeA"


def get_ssh_client(username: str, password: str) -> paramiko.SSHClient:
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(
        hostname=SABALCORE_HOST,
        port=SABALCORE_PORT,
        username=username,
        password=password,
        timeout=15
    )
    return client


def run_command(client: paramiko.SSHClient, command: str) -> tuple[str, str]:
    stdin, stdout, stderr = client.exec_command(command)
    return stdout.read().decode().strip(), stderr.read().decode().strip()


@app.post("/submit")
async def submit_job(
    sim_file: UploadFile,
    username: str = Form(...),
    password: str = Form(...),
    job_name: str = Form("BFR_run"),
    run_mode: str = Form("mesh,run"),
    nodes: int = Form(8),
    ppn: int = Form(16),
):
    sim_filename = sim_file.filename
    sim_contents = await sim_file.read()

    pbs_content = (
        f"#PBS -l nodes={nodes}:red:ppn={ppn}\n"
        f"#PBS -l nodes+={nodes}:copper:ppn={ppn}\n"
        f"#PBS -l nodes++={nodes}:blue:ppn={ppn}\n"
        f"#PBS -N {job_name}\n\n"
        f"cd $PBS_O_WORKDIR\n\n"
        f"module load {STARCCM_MODULE}\n\n"
        f"starccm+ -power -podkey {POD_KEY} -np $PBS_NP -batch {run_mode} {sim_filename}\n"
    )

    try:
        client = get_ssh_client(username, password)
    except paramiko.AuthenticationException:
        raise HTTPException(status_code=401, detail="Invalid Sabalcore username or password.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not connect to Sabalcore: {str(e)}")

    try:
        sftp = client.open_sftp()

        try:
            sftp.stat("bfr")
        except FileNotFoundError:
            sftp.mkdir("bfr")

        sftp.putfo(io.BytesIO(sim_contents), f"bfr/{sim_filename}")
        sftp.putfo(io.BytesIO(pbs_content.encode()), "bfr/run.pbs")
        sftp.close()

        run_command(client, "dos2unix ~/bfr/run.pbs")
        stdout, stderr = run_command(client, "cd ~/bfr && qsub run.pbs")

        if not stdout:
            raise HTTPException(status_code=500, detail=f"qsub failed: {stderr}")

        job_id = stdout.strip()
        return JSONResponse({"job_id": job_id, "sim_file": sim_filename})

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Job submission failed: {str(e)}")
    finally:
        client.close()


@app.get("/status")
async def job_status(
    username: str,
    password: str,
    job_id: str,
):
    try:
        client = get_ssh_client(username, password)
    except paramiko.AuthenticationException:
        raise HTTPException(status_code=401, detail="Invalid credentials.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    try:
        stdout, _ = run_command(client, f"qstat -f {job_id}")

        state = "unknown"
        walltime = "00:00:00"
        nodes_used = ""

        for line in stdout.splitlines():
            line = line.strip()
            if "job_state" in line:
                state = line.split("=")[-1].strip()
            if "resources_used.walltime" in line:
                walltime = line.split("=")[-1].strip()
            if "Resource_List.nodes " in line:
                nodes_used = line.split("=")[-1].strip()

        if not stdout:
            state = "C"

        tail_out = ""
        if state == "R":
            tail_stdout, _ = run_command(
                client,
                f"ls ~/.pbs_spool/{job_id}.OU 2>/dev/null && tail -20 ~/.pbs_spool/{job_id}.OU || echo ''"
            )
            tail_out = tail_stdout

        if state == "C":
            job_num = job_id.split(".")[0]
            tail_stdout, _ = run_command(
                client,
                f"ls ~/bfr/BFR_run.o{job_num} 2>/dev/null && tail -20 ~/bfr/BFR_run.o{job_num} || echo ''"
            )
            tail_out = tail_stdout

        return JSONResponse({
            "job_id": job_id,
            "state": state,
            "walltime": walltime,
            "nodes": nodes_used,
            "output_tail": tail_out
        })

    finally:
        client.close()


@app.post("/kill")
async def kill_job(
    username: str = Form(...),
    password: str = Form(...),
    job_id: str = Form(...),
):
    try:
        client = get_ssh_client(username, password)
    except paramiko.AuthenticationException:
        raise HTTPException(status_code=401, detail="Invalid credentials.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    try:
        stdout, stderr = run_command(client, f"qdel {job_id}")
        return JSONResponse({"killed": True, "job_id": job_id})
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to kill job: {str(e)}")
    finally:
        client.close()


@app.get("/health")
async def health():
    return {"status": "ok"}
