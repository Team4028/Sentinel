function registerServiceWorker(serviceWorkerUrl) {
    let swRegistration = null;
    if ('serviceWorker' in navigator && 'PushManager' in window) {
        console.log('Service Worker and Push is supported');

        navigator.serviceWorker.register(serviceWorkerUrl); // make SW
        navigator.serviceWorker.ready.then(function (swReg) { // when it is done being created...
            console.log('Service Worker is registered', swReg);
            swReg.active.postMessage({
                type: "SET_CSRF",
                token: csrfToken
            });
            swRegistration = swReg;
        })
            .catch(function (error) {
                console.error('Service Worker Error', error);
            });
    } else {
        console.warn('Push messaging is not supported');
    }
    return swRegistration;
}