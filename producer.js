const { Kafka } = require("kafkajs");
const readline = require("readline");

const kafka = new Kafka({
    clientId: "my-app",
    brokers: ["localhost:9092"], // Update the broker to localhost
});
const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
});

const producer = kafka.producer();

const produceMessage = async () => {
    await producer.connect();
    console.log("Producer connected!");

    rl.setPrompt("> ");
    rl.prompt();

    rl.on("line", async function (line) {
        const [riderName, location] = line.split(" ");
        await producer.send({
            topic: "test-topic",
            messages: [
                {
                    // partition: location.toLowerCase() === "north" ? 0 : 1,
                    key: "location-update",
                    value: JSON.stringify({ name: riderName, location }),
                },
            ],
        });
    }).on("close", async () => {
        await producer.disconnect();
    });

    // setInterval(async () => {
    //     try {
    //         const message = { value: `Hello Kafka ${new Date().toISOString()}` };
    //         await producer.send({
    //             topic: "test-topic",
    //             messages: [message],
    //         });
    //         console.log(`Message sent: ${message.value}`);
    //     } catch (error) {
    //         console.error("Error sending message:", error);
    //     }
    // }, 5000);
};

produceMessage().catch(console.error);
