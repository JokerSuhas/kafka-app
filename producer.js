const { kafka } = require('./client');
const readline = require('readline');

const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
});

async function init() {
    const producer = kafka.producer();

    try {
        console.log('Connecting producer');
        await producer.connect();
        console.log('Producer connected');

        rl.setPrompt("> ");
        rl.prompt();

        rl.on('line', async function(line) {
            const [riderName, location] = line.split(' ');

            await producer.send({
                topic: "rider-updates",
                messages: [
                    {
                        key: "location update",
                        value: JSON.stringify({ name: riderName, location }),
                    },
                ],
                partitions: location.toLowerCase() === "north" ? [0] : [1]
            });
        });

        rl.on('close', async () => {
            await producer.disconnect();
            console.log('Producer disconnected');
            process.exit(0);
        });

    } catch (error) {
        console.error('Error sending message:', error);
    }
}

init();
