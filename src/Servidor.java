import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

class Servidor {
    private String meuIP;
    private int minhaPorta;
    private String liderIP;
    private int liderPorta;
    private boolean souLider;
    private Map<String, String> tabelaHash;
    private Map<String, Long> timestamps;
    private long contadorTimestamp;
    private final Object lock = new Object();
    private ExecutorService threadPool;

    // Para controlar confirmações de replicação
    private Map<String, Integer> replicationConfirmations;
    private Map<String, Mensagem> pendingPutResponses;
    private Map<String, ClientConnection> pendingClientConnections;

    // Classe para manter conexões de cliente abertas
    private static class ClientConnection {
        Socket socket;
        ObjectOutputStream out;
        ObjectInputStream in;
        String clienteIP;
        int clientePorta;

        ClientConnection(Socket socket, ObjectOutputStream out, ObjectInputStream in, String clienteIP, int clientePorta) {
            this.socket = socket;
            this.out = out;
            this.in = in;
            this.clienteIP = clienteIP;
            this.clientePorta = clientePorta;
        }
    }

    public Servidor() {
        this.tabelaHash = new HashMap<>();
        this.timestamps = new HashMap<>();
        this.contadorTimestamp = 0;
        this.threadPool = Executors.newCachedThreadPool();
        this.replicationConfirmations = new HashMap<>();
        this.pendingPutResponses = new HashMap<>();
        this.pendingClientConnections = new HashMap<>();
    }

    public void inicializar() {
        Scanner scanner = new Scanner(System.in);

        System.out.print("Digite o IP deste servidor (padrão: 127.0.0.1): ");
        String ip = scanner.nextLine().trim();
        this.meuIP = ip.isEmpty() ? "127.0.0.1" : ip;

        System.out.print("Digite a porta deste servidor: ");
        this.minhaPorta = Integer.parseInt(scanner.nextLine());

        System.out.print("Digite o IP do líder (padrão: 127.0.0.1): ");
        String liderIPInput = scanner.nextLine().trim();
        this.liderIP = liderIPInput.isEmpty() ? "127.0.0.1" : liderIPInput;

        System.out.print("Digite a porta do líder: ");
        this.liderPorta = Integer.parseInt(scanner.nextLine());

        this.souLider = (this.meuIP.equals(this.liderIP) && this.minhaPorta == this.liderPorta);

        System.out.println("Servidor iniciado em " + meuIP + ":" + minhaPorta);
        System.out.println("Líder: " + liderIP + ":" + liderPorta);
        System.out.println("Sou líder: " + souLider);
    }

    public void iniciarServidor() {
        try (ServerSocket serverSocket = new ServerSocket(minhaPorta)) {
            System.out.println("Servidor ouvindo na porta " + minhaPorta);

            while (true) {
                Socket clientSocket = serverSocket.accept();
                threadPool.submit(new ClientHandler(clientSocket));
            }
        } catch (IOException e) {
            System.err.println("Erro no servidor: " + e.getMessage());
        }
    }

    private class ClientHandler implements Runnable {
        private Socket socket;

        public ClientHandler(Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {
            ObjectInputStream in = null;
            ObjectOutputStream out = null;
            boolean manterConexaoAberta = false;
            
            try {
                in = new ObjectInputStream(socket.getInputStream());
                out = new ObjectOutputStream(socket.getOutputStream());

                Mensagem mensagem = (Mensagem) in.readObject();

                switch (mensagem.getTipo()) {
                    case PUT:
                        manterConexaoAberta = processarPUT(mensagem, out, socket, in);
                        break;
                    case GET:
                        processarGET(mensagem, out, socket);
                        break;
                    case REPLICATION:
                        processarREPLICATION(mensagem, out);
                        break;
                    case REPLICATION_OK:
                        processarREPLICATION_OK(mensagem);
                        break;
                }

            } catch (Exception e) {
                System.err.println("Erro ao processar cliente: " + e.getMessage());
                e.printStackTrace();
            } finally {
                // Só fechar se não precisar manter aberta para PUT do líder
                if (!manterConexaoAberta) {
                    try {
                        if (in != null) in.close();
                        if (out != null) out.close();
                        if (!socket.isClosed()) socket.close();
                    } catch (Exception e) {
                        // Ignorar erros de fechamento
                    }
                }
            }
        }
    }

    private boolean processarPUT(Mensagem mensagem, ObjectOutputStream out, Socket socket, ObjectInputStream in) throws Exception {
        String clienteIP = socket.getInetAddress().getHostAddress();
        int clientePorta = socket.getPort();

        if (!souLider) {
            System.out.println("Encaminhando PUT key:" + mensagem.getKey() + " value:" + mensagem.getValue());

            // Encaminhar para o líder
            try (Socket liderSocket = new Socket(liderIP, liderPorta);
                    ObjectOutputStream liderOut = new ObjectOutputStream(liderSocket.getOutputStream());
                    ObjectInputStream liderIn = new ObjectInputStream(liderSocket.getInputStream())) {

                Mensagem putParaLider = new Mensagem(Mensagem.TipoMensagem.PUT, mensagem.getKey(), mensagem.getValue());
                putParaLider.setClienteIP(clienteIP);
                putParaLider.setClientePorta(clientePorta);
                liderOut.writeObject(putParaLider);

                Mensagem respostaLider = (Mensagem) liderIn.readObject();
                out.writeObject(respostaLider);
            }
            return false; // Não manter conexão aberta
        } else {
            System.out.println("Cliente " + clienteIP + ":" + clientePorta + " PUT key:" + mensagem.getKey() + " value:"
                    + mensagem.getValue());

            synchronized (lock) {
                contadorTimestamp++;
                tabelaHash.put(mensagem.getKey(), mensagem.getValue());
                timestamps.put(mensagem.getKey(), contadorTimestamp);

                // Contar quantos servidores não-líderes existem
                int[] portas = { 10097, 10098, 10099 };
                int numServidoresNaoLider = 0;
                for (int porta : portas) {
                    if (porta != minhaPorta) {
                        numServidoresNaoLider++;
                    }
                }

                // Se não há outros servidores para replicar, enviar PUT_OK imediatamente
                if (numServidoresNaoLider == 0) {
                    Mensagem putOk = new Mensagem(Mensagem.TipoMensagem.PUT_OK, mensagem.getKey(), mensagem.getValue(),
                            contadorTimestamp);
                    out.writeObject(putOk);
                    System.out.println("Enviando PUT_OK ao Cliente " + clienteIP + ":" + clientePorta + " da key:"
                            + mensagem.getKey() + " ts:" + contadorTimestamp);
                    return false; // Não manter conexão aberta
                } else {
                    // Criar chave única para este PUT
                    String putKey = mensagem.getKey() + "_" + contadorTimestamp;

                    // Inicializar contador de confirmações
                    replicationConfirmations.put(putKey, 0);
                    replicationConfirmations.put(putKey + "_total", numServidoresNaoLider);

                    // Preparar resposta para enviar após confirmações
                    Mensagem putOk = new Mensagem(Mensagem.TipoMensagem.PUT_OK, mensagem.getKey(), mensagem.getValue(),
                            contadorTimestamp);
                    pendingPutResponses.put(putKey, putOk);
                    
                    // Armazenar conexão do cliente (IMPORTANTE: manter socket aberto)
                    ClientConnection clientConn = new ClientConnection(socket, out, in, clienteIP, clientePorta);
                    pendingClientConnections.put(putKey, clientConn);

                    // Replicar para outros servidores
                    replicarParaOutrosServidores(mensagem.getKey(), mensagem.getValue(), contadorTimestamp, putKey,
                            numServidoresNaoLider);
                    
                    return true; // Manter conexão aberta para enviar PUT_OK depois
                }
            }
        }
    }

    private void processarGET(Mensagem mensagem, ObjectOutputStream out, Socket socket) throws Exception {
        String clienteIP = socket.getInetAddress().getHostAddress();
        int clientePorta = socket.getPort();

        synchronized (lock) {
            String key = mensagem.getKey();
            long timestampCliente = mensagem.getTimestamp();
            long timestampServidor = timestamps.getOrDefault(key, 0L);

            System.out.println(
                    "Cliente " + clienteIP + ":" + clientePorta + " GET key:" + key + " ts:" + timestampCliente +
                            ". Meu ts é " + timestampServidor + ", portanto devolvendo " +
                            (timestampServidor >= timestampCliente ? tabelaHash.getOrDefault(key, "NULL")
                                    : "WAIT_FOR_RESPONSE"));

            if (timestampServidor >= timestampCliente) {
                String value = tabelaHash.getOrDefault(key, "NULL");
                Mensagem resposta = new Mensagem(Mensagem.TipoMensagem.GET_RESPONSE, key, value, timestampServidor);
                out.writeObject(resposta);
            } else {
                Mensagem waitMsg = new Mensagem(Mensagem.TipoMensagem.WAIT_FOR_RESPONSE);
                out.writeObject(waitMsg);

                // Implementar lógica de espera assíncrona para atualização
                // Aguardar atualização assíncrona
                threadPool.submit(() -> {
                    try {
                        while (true) {
                            Thread.sleep(100);
                            synchronized (lock) {
                                long novoTimestamp = timestamps.getOrDefault(key, 0L);
                                if (novoTimestamp >= timestampCliente) {
                                    String value = tabelaHash.getOrDefault(key, "NULL");

                                    // Para implementação correta, seria necessário implementar callback assíncrono
                                    // Por enquanto, retornando resposta direta
                                    break;
                                }
                            }
                        }
                    } catch (Exception e) {
                        System.err.println("Erro na resposta assíncrona: " + e.getMessage());
                    }
                });
            }
        }
    }

    private void processarREPLICATION(Mensagem mensagem, ObjectOutputStream out) throws Exception {
        System.out.println("REPLICATION key:" + mensagem.getKey() + " value:" + mensagem.getValue() + " ts:"
                + mensagem.getTimestamp());

        synchronized (lock) {
            tabelaHash.put(mensagem.getKey(), mensagem.getValue());
            timestamps.put(mensagem.getKey(), mensagem.getTimestamp());
        }

        Mensagem replicationOk = new Mensagem(Mensagem.TipoMensagem.REPLICATION_OK);
        replicationOk.setKey(mensagem.getKey());
        replicationOk.setTimestamp(mensagem.getTimestamp());
        out.writeObject(replicationOk);
    }

    private void processarREPLICATION_OK(Mensagem mensagem) {
        if (souLider) {
            synchronized (lock) {
                // Criar chave única para identificar este PUT
                String putKey = mensagem.getKey() + "_" + mensagem.getTimestamp();

                // Incrementar contador de confirmações
                Integer confirmacoes = replicationConfirmations.get(putKey);
                if (confirmacoes != null) {
                    confirmacoes++;
                    replicationConfirmations.put(putKey, confirmacoes);

                    // Verificar se recebeu confirmação de todos os servidores
                    Integer totalEsperado = replicationConfirmations.get(putKey + "_total");
                    if (totalEsperado == null)
                        totalEsperado = 2; // fallback

                    if (confirmacoes >= totalEsperado) {
                        // Enviar PUT_OK para o cliente
                        Mensagem putOk = pendingPutResponses.get(putKey);
                        ClientConnection clientConn = pendingClientConnections.get(putKey);

                        if (putOk != null && clientConn != null) {
                            try {
                                if (!clientConn.socket.isClosed()) {
                                    clientConn.out.writeObject(putOk);
                                    clientConn.out.flush(); // Garantir que a mensagem seja enviada

                                    System.out.println("Enviando PUT_OK ao Cliente " + clientConn.clienteIP + ":" + clientConn.clientePorta +
                                            " da key:" + putOk.getKey() + " ts:" + putOk.getTimestamp());
                                } else {
                                    System.err.println("Socket do cliente está fechado para PUT key:" + putOk.getKey());
                                }
                            } catch (Exception e) {
                                System.err.println("Erro ao enviar PUT_OK para cliente: " + e.getMessage());
                            } finally {
                                // Agora podemos fechar o socket do cliente
                                try {
                                    if (clientConn.in != null) clientConn.in.close();
                                    if (clientConn.out != null) clientConn.out.close();
                                    if (!clientConn.socket.isClosed()) clientConn.socket.close();
                                } catch (Exception e) {
                                    // Ignorar erros de fechamento
                                }
                            }
                        }

                        // Limpar estruturas pendentes
                        replicationConfirmations.remove(putKey);
                        replicationConfirmations.remove(putKey + "_total");
                        pendingPutResponses.remove(putKey);
                        pendingClientConnections.remove(putKey);
                    }
                }
            }
        }
    }

    private void replicarParaOutrosServidores(String key, String value, long timestamp, String putKey,
            int numServidoresEsperados) {
        // Portas padrão dos servidores
        int[] portas = { 10097, 10098, 10099 };

        for (int porta : portas) {
            if (porta != minhaPorta) {
                threadPool.submit(() -> {
                    try (Socket socket = new Socket("127.0.0.1", porta);
                            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                            ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {

                        Mensagem replicationMsg = new Mensagem(Mensagem.TipoMensagem.REPLICATION, key, value,
                                timestamp);
                        out.writeObject(replicationMsg);

                        Mensagem resposta = (Mensagem) in.readObject();

                        if (resposta.getTipo() == Mensagem.TipoMensagem.REPLICATION_OK) {
                            resposta.setKey(key);
                            resposta.setTimestamp(timestamp);
                            processarREPLICATION_OK(resposta);
                        }

                    } catch (Exception e) {
                        System.err.println("Erro ao replicar para servidor na porta " + porta + ": " + e.getMessage());

                        // Em caso de erro, ainda processar como se fosse uma confirmação
                        // para evitar que o cliente fique esperando indefinidamente
                        Mensagem errorResponse = new Mensagem(Mensagem.TipoMensagem.REPLICATION_OK);
                        errorResponse.setKey(key);
                        errorResponse.setTimestamp(timestamp);
                        processarREPLICATION_OK(errorResponse);
                    }
                });
            }
        }
    }

    public void executar() {
        inicializar();
        iniciarServidor();
    }

    public static void main(String[] args) {
        Servidor servidor = new Servidor();
        servidor.executar();
    }
}