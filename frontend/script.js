const API_URL = "http://localhost:5000";

let commandHistory = [];


async function executeCommand() {

    const routerIp =
        document.getElementById("router").value;

    const command =
        document.getElementById("command").value.trim();

    const button =
        document.getElementById("executeBtn");

    const output =
        document.getElementById("output");

    const status =
        document.getElementById("status");


    // Check command
    if (!command) {

        status.textContent =
            "Please enter a command.";

        return;
    }


    // Disable button
    button.disabled = true;

    status.textContent =
        "Sending command...";

    output.textContent =
        "Waiting for router response...";


    try {

        // ==========================================
        // STEP 1: Send command to Flask
        // ==========================================

        const response = await fetch(
            `${API_URL}/command`,
            {
                method: "POST",

                headers: {
                    "Content-Type": "application/json"
                },

                body: JSON.stringify({
                    router_ip: routerIp,
                    command: command
                })
            }
        );


        const data = await response.json();


        if (!response.ok) {

            throw new Error(
                data.error || "Request failed"
            );
        }


        // Get request ID
        const requestId = data.request_id;


        status.textContent =
            "Command sent. Waiting for router...";


        // ==========================================
        // STEP 2: Wait for router result
        // ==========================================

        await waitForResult(
            requestId,
            output,
            status
        );


        // Add to history
        addHistory(
            routerIp,
            command
        );


    } catch (error) {

        console.error(error);

        status.textContent =
            "Error: " + error.message;

        output.textContent =
            "Could not get router response.";

    } finally {

        button.disabled = false;

    }
}


// ==========================================
// Wait for router result
// ==========================================

async function waitForResult(
    requestId,
    output,
    status
) {

    const maxAttempts = 30;

    for (
        let attempt = 0;
        attempt < maxAttempts;
        attempt++
    ) {

        try {

            const response = await fetch(
                `${API_URL}/result/${requestId}`
            );


            const data = await response.json();


            // ==================================
            // Result is ready
            // ==================================

            if (
                response.ok &&
                data.status === "success"
            ) {

                status.textContent =
                    "Command completed successfully.";

                output.textContent =
                    data.output || "No output returned.";

                return;
            }


            // ==================================
            // Router error
            // ==================================

            if (data.status === "error") {

                status.textContent =
                    "Router command failed.";

                output.textContent =
                    data.error || "Unknown router error.";

                return;
            }


            // ==================================
            // Still waiting
            // ==================================

            status.textContent =
                "Waiting for router...";


            // Wait 500 milliseconds
            await sleep(500);

        } catch (error) {

            throw new Error(
                "Could not check router result."
            );
        }
    }


    // ==========================================
    // Timeout
    // ==========================================

    status.textContent =
        "Request timed out.";

    output.textContent =
        "The router did not return a result.";
}


// ==========================================
// Sleep helper
// ==========================================

function sleep(milliseconds) {

    return new Promise(
        resolve =>
            setTimeout(resolve, milliseconds)
    );
}


// ==========================================
// Command history
// ==========================================

function addHistory(
    routerIp,
    command
) {

    commandHistory.unshift({
        routerIp: routerIp,
        command: command
    });

    renderHistory();
}


// ==========================================
// Display history
// ==========================================

function renderHistory() {

    const history =
        document.getElementById("history");


    if (commandHistory.length === 0) {

        history.textContent =
            "No commands executed yet.";

        return;
    }


    history.innerHTML = "";


    commandHistory.forEach(item => {

        const div =
            document.createElement("div");

        div.className =
            "history-item";


        div.innerHTML = `
            <div class="history-router">
                Router: ${item.routerIp}
            </div>

            <div class="history-command">
                ${item.command}
            </div>
        `;


        history.appendChild(div);

    });
}