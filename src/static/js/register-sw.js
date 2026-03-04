function stimulateSW(worker) {
    worker.postMessage({
        type: "Stimulation",
        name: "lonely wizard"
    });
}

function registerServiceWorker(serviceWorkerUrl) {
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