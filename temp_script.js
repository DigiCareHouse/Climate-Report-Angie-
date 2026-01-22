
// Progress Bar & Async Form Submission
document.addEventListener('DOMContentLoaded', function () {
    const mainForm = document.querySelector('form');
    if (!mainForm) return;

    mainForm.addEventListener('submit', async function (e) {
        e.preventDefault();

        const form = this;
        const formData = new FormData(form);
        const submitBtn = form.querySelector('button[type="submit"]');

        // Prepare UI
        if (submitBtn) {
            submitBtn.disabled = true;
            submitBtn.innerHTML = '<span class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span> Processing...';
        }

        const overlay = document.getElementById('progressOverlay');
        const progressBar = document.getElementById('progressBar');
        const progressPercent = document.getElementById('progressPercent');
        const progressMessage = document.getElementById('progressMessage');
        const completionIcon = document.getElementById('completionIcon');
        const downloadAction = document.getElementById('downloadAction');
        const downloadLink = document.getElementById('downloadLink');

        // Show Overlay
        overlay.style.display = 'flex';
        // Force reflow
        overlay.offsetHeight;
        overlay.classList.add('visible');

        try {
            // Start Process
            const response = await fetch('/process', {
                method: 'POST',
                body: formData
            });

            const data = await response.json();

            if (data.error) {
                throw new Error(data.error);
            }

            const taskId = data.task_id;

            // Poll Progress
            const pollInterval = setInterval(async () => {
                try {
                    const statusRes = await fetch(`/progress/${taskId}`);
                    const statusData = await statusRes.json();

                    // Update UI
                    const percent = statusData.percent || 0;
                    progressBar.style.width = `${percent}%`;
                    progressPercent.textContent = `${percent}%`;
                    progressMessage.textContent = statusData.message || 'Processing...';

                    if (statusData.status === 'completed') {
                        clearInterval(pollInterval);
                        progressBar.style.width = '100%';
                        progressPercent.textContent = '100%';
                        progressMessage.textContent = 'Report Ready!';

                        // Show Success UI
                        if (completionIcon) completionIcon.style.display = 'block';
                        if (downloadAction) {
                            downloadLink.href = `/download/${statusData.result_file}`;
                            downloadAction.style.display = 'block';
                        }

                    } else if (statusData.status === 'error') {
                        clearInterval(pollInterval);
                        progressMessage.textContent = `Error: ${statusData.message}`;
                        progressMessage.style.color = '#dc3545';
                        progressBar.style.backgroundColor = '#dc3545';
                        if (submitBtn) {
                            submitBtn.disabled = false;
                            submitBtn.innerHTML = 'Try Again';
                        }
                    }
                } catch (err) {
                    console.error("Polling error", err);
                }
            }, 1000); // Poll every second

        } catch (error) {
            console.error("Submission error", error);
            progressMessage.textContent = `Error: ${error.message}`;
            if (submitBtn) {
                submitBtn.disabled = false;
                submitBtn.innerHTML = 'Generate Report';
            }
        }
    });
});
