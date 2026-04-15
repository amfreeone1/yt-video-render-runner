import os


@app.get("/render-jobs/{render_job_key}/artifact")
def download_artifact(render_job_key: str):
    job = jobs.get(render_job_key)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job["status"] != "completed":
        raise HTTPException(status_code=400, detail=f"Job not completed. Status: {job['status']}")

    output_path = f"/tmp/{render_job_key}.mp4"

    if not os.path.exists(output_path):
        raise HTTPException(status_code=404, detail="Output file not found on disk")

    def iter_file():
        with open(output_path, "rb") as f:
            while chunk := f.read(65536):
                yield chunk

    return StreamingResponse(
        iter_file(),
        media_type="video/mp4",
        headers={"Content-Disposition": f"attachment; filename={render_job_key}.mp4"}
    )
