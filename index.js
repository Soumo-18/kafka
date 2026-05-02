import 'dotenv/config'
import http from 'node:http'
import path from 'node:path'

import express from 'express'
import { Server } from 'socket.io'
import cookieParser from 'cookie-parser'
import { kafkaClient } from './kafka-client.js'

const CLIENT_ID = process.env.CLIENT_ID; 
const CLIENT_SECRET = process.env.CLIENT_SECRET;
const REDIRECT_URI = process.env.REDIRECT_URI;
const OIDC_ISSUER = process.env.OIDC_ISSUER;

async function main() {
    const PORT = process.env.PORT ?? 8000

    const app = express()
    const server = http.createServer(app)
    const io = new Server (server)

    app.use(cookieParser())

    const kafkaProducer = kafkaClient.producer()
    await kafkaProducer.connect()

    const kafkaConsumer = kafkaClient.consumer({ groupId:`scoket-server-${PORT}`})
    await kafkaConsumer.connect()

    await kafkaConsumer.subscribe({ 
        topics:['location-updates'],
         fromBeginning:true 
    })
    kafkaConsumer.run({
        eachMessage: async ({ topic, partition, message, heartbeat}) => {   //whenever we get a msg this callback func will run
            const data = JSON.parse(message.value.toString())
            console.log(`Kafka Consumer Data Received`, { data })
            io.emit('server:location-update', {
                id: data.id, //this will be User's name
                 lat:data.lat,
                  lng:data.lng 
            } )
            await heartbeat()
        },
    })

    io.attach(server)


    // <-- 4. ADDED LOGIN & CALLBACK ROUTES -->
    app.get('/login', (req, res) => {
        const state = Math.random().toString(36).substring(7)
        const authUrl = new URL(`${OIDC_ISSUER}/o/authenticate`)
        authUrl.searchParams.append('client_id', CLIENT_ID)
        authUrl.searchParams.append('redirect_uri', REDIRECT_URI)
        authUrl.searchParams.append('state', state)
        res.redirect(authUrl.toString())
    })

    app.get('/api/auth/callback', async (req, res) => {
        const { code } = req.query
        if (!code) return res.status(400).send('No code provided.')

        try {
            const tokenRes = await fetch(`${OIDC_ISSUER}/o/token`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    grant_type: 'authorization_code',
                    code,
                    client_id: CLIENT_ID,
                    client_secret: CLIENT_SECRET
                })
            })

            if (!tokenRes.ok) {
                const errorHtml = await tokenRes.text();
                console.error(`Auth Server Failed with Status ${tokenRes.status}:`, errorHtml.substring(0, 200)); 
                throw new Error('OIDC Server returned an error');
            }
            
            const tokenData = await tokenRes.json()
            if (!tokenData.id_token) throw new Error('Auth failed')

            // Save token securely in browser cookie
            res.cookie('auth_token', tokenData.id_token, { httpOnly: true })
            res.redirect('/') // Send back to the map
        } catch (error) {
            console.error("Auth Callback Error Details:", error);
            res.status(500).send('Authentication failed')
        }
    })

    // <-- 5. ADDED ROUTE PROTECTION -->
    // This blocks access to the map unless the user is logged in
    app.use((req, res, next) => {
        if (req.path === '/health') return next()
        const token = req.cookies.auth_token
        if (!token) return res.redirect('/login')
        
        try {
            // Decode the JWT token to get user info
            const payload = JSON.parse(Buffer.from(token.split('.')[1], 'base64').toString())
            req.user = payload
            next()
        } catch (e) {
            res.clearCookie('auth_token')
            res.redirect('/login')
        }
    })
    

    io.use((socket, next) => {
        const cookieHeader = socket.request.headers.cookie
        if (!cookieHeader) return next(new Error('Auth error'))
        
        const cookies = Object.fromEntries(cookieHeader.split('; ').map(c => c.split('=')))
        if (!cookies.auth_token) return next(new Error('Auth error'))

        try {
            const payload = JSON.parse(Buffer.from(cookies.auth_token.split('.')[1], 'base64').toString())
            socket.data.user = payload // Attach user to socket
            next()
        } catch (e) {
            next(new Error('Auth error'))
        }
    })





    io.on('connection', (socket) => {
        const userId = socket.data.user.name || socket.data.user.sub

        console.log(`[User:${userId}]: Connected Successfully`)

        socket.on('client:location:update', async (locationData) => {
            const { lat, lng } = locationData
            console.log(`[User:${userId}]: location updated`)

            await kafkaProducer.send({ topic:'location-updates', 
                messages:[
                    {
                        key:userId,
                        value:JSON.stringify({id: userId, lat, lng })
                    }
                ]
            })
        })
        socket.on('disconnect', () => {
            console.log(`[User:${userId}]: Disconnected`)
            io.emit('server:user-disconnected', userId);
        })
    })


    app.use(express.static(path.resolve('./public')))

    app.get('/health', (req,res) => {
        return res.json({ healthy:true})
    })

    server.listen(PORT, () => console.log(`Server Running on http://localhost:${PORT}`))
}


main()