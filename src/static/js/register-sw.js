function stimulateSW(worker) {
    worker.postMessage({
        type: "Stimulation",
        name: "lonely wizard"
    });
}

navigator.serviceWorker.addEventListener("message", async (message) => {
    if (message.data?.type === "COPY_CLIP") {
        try {
            await navigator.clipboard.writeText(message.data.text);
            console.log("Copied from SW!");
        } catch (err) {
            console.error("Failed to copy message: ", err);
        }
    }
});

function registerServiceWorker(serviceWorkerUrl) {
    if (window.self !== window.top) return;
    console.log("Window is top, registering SW");
    let swRegistration = null;
    if ('serviceWorker' in navigator && 'PushManager' in window) {
        console.log('Service Worker and Push is supported');
        navigator.serviceWorker.register(serviceWorkerUrl); // make SW
        navigator.serviceWorker.ready.then(function (swReg) { // when it is done being created...
            console.log('Service Worker is registered', swReg);
            swRegistration = swReg;
            setInterval(() => stimulateSW(swReg.active), 30_000); // try and keep the SW from falling asleep
            swReg.active.postMessage({
                data: "Wake Up"
            })
        })
            .catch(function (error) {
                console.error('Service Worker Error', error);
            });
    } else {
        console.warn('Push messaging is not supported');
    }
    return swRegistration;
}