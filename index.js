import http from 'node:http'
import path from 'node:path'

import express from 'express'
import { Server } from 'socket.io'

async function main() {
    const PORT = process.env.PORT ?? 8000

    const app = express()
    const server = http.createServer(app)
    const io = new Server (server)

    io.attach(server)


    io.on('connection', (socket) => {
        console.log(`[Socket:${socket.id}]: Connected Successfully`)

        socket.on('client:location:update', async (locationData) => {
            const { lat, lng } = locationData
            console.log(
                `[Socket:${socket.id}]:client:location:update: `,
                 locationData
            )
        })
    })


    app.use(express.static(path.resolve('./public')))

    app.get('/health', (req,res) => {
        return res.json({ healthy:true})
    })

    server.listen(PORT, () => console.log(`Server Running on http://localhost:${PORT}`))
}


main()