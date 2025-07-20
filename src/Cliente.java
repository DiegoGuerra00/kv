import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

class Cliente {
    private String[] servidorIPs;
    private int[] servidorPortas;
    private Map<String, Long> timestamps;
    private Random random;
    private ExecutorService threadPool;
    private ServerSocket callbackServer;
    private int callbackPorta;

    public Cliente() {
        this.servidorIPs = new String[3];
        this.servidorPortas = new int[3];
        this.timestamps = new HashMap<>();
        this.random = new Random();
        this.threadPool = Executors.newCachedThreadPool();
    }

    public void inicializar() {
        Scanner scanner = new Scanner(System.in);

        System.out.println("=== INICIALIZAÇÃO DO CLIENTE ===");
        for (int i = 0; i < 3; i++) {
            System.out.print("Digite o IP do servidor " + (i + 1) + " (padrão: 127.0.0.1): ");
            String ip = scanner.nextLine().trim();
            if (ip.isEmpty()) {
                ip = "127.0.0.1";
            }
            servidorIPs[i] = ip;

            System.out.print("Digite a porta do servidor " + (i + 1) + " (padrão: " + (10097 + i) + "): ");
            String portaStr = scanner.nextLine().trim();
            if (portaStr.isEmpty()) {
                servidorPortas[i] = 10097 + i;
            } else {
                servidorPortas[i] = Integer.parseInt(portaStr);
            }
        }

        // Inicializar servidor de callback para respostas assíncronas
        try {
            callbackServer = new ServerSocket(0); // Porta automática
            callbackPorta = callbackServer.getLocalPort();
            System.out.println("Servidor de callback iniciado na porta: " + callbackPorta);
            
            // Iniciar thread para aceitar callbacks assíncronos
            threadPool.submit(new CallbackListener());
            
        } catch (Exception e) {
            System.err.println("Erro ao inicializar servidor de callback: " + e.getMessage());
        }

        System.out.println("Cliente inicializado com sucesso!");
    }

    private class CallbackListener implements Runnable {
        @Override
        public void run() {
            while (!callbackServer.isClosed()) {
                try {
                    Socket clientSocket = callbackServer.accept();
                    threadPool.submit(new CallbackHandler(clientSocket));
                } catch (Exception e) {
                    if (!callbackServer.isClosed()) {
                        System.err.println("Erro ao aceitar callback: " + e.getMessage());
                    }
                    break;
                }
            }
        }
    }

    private class CallbackHandler implements Runnable {
        private Socket socket;

        public CallbackHandler(Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {
            try (ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {
                Mensagem resposta = (Mensagem) in.readObject();
                
                if (resposta.getTipo() == Mensagem.TipoMensagem.GET_RESPONSE) {
                    // Atualizar timestamp local
                    timestamps.put(resposta.getKey(), resposta.getTimestamp());
                    
                    // Imprimir resposta assíncrona
                    System.out.println("\n[RESPOSTA ASSÍNCRONA] GET key: " + resposta.getKey() + 
                                     " value: " + resposta.getValue() +
                                     " obtido do servidor de forma assíncrona" +
                                     ", meu timestamp anterior era menor" +
                                     " e do servidor " + resposta.getTimestamp());
                    System.out.print("Digite uma opção: "); // Reexibir prompt
                }
                
            } catch (Exception e) {
                System.err.println("Erro ao processar callback: " + e.getMessage());
            } finally {
                try {
                    socket.close();
                } catch (Exception e) {
                    // Ignorar erro de fechamento
                }
            }
        }
    }

    public void executarPUT() {
        Scanner scanner = new Scanner(System.in);

        System.out.print("Digite a key: ");
        String key = scanner.nextLine();

        System.out.print("Digite o value: ");
        String value = scanner.nextLine();

        int servidorEscolhido = random.nextInt(3);
        String ip = servidorIPs[servidorEscolhido];
        int porta = servidorPortas[servidorEscolhido];

        try (Socket socket = new Socket(ip, porta);
                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {

            Mensagem putMsg = new Mensagem(Mensagem.TipoMensagem.PUT, key, value);
            out.writeObject(putMsg);

            Mensagem resposta = (Mensagem) in.readObject();

            if (resposta.getTipo() == Mensagem.TipoMensagem.PUT_OK) {
                timestamps.put(key, resposta.getTimestamp());
                System.out.println("PUT_OK key: " + key + " value " + value +
                        " timestamp " + resposta.getTimestamp() +
                        " realizada no servidor " + ip + ":" + porta);
            }

        } catch (Exception e) {
            System.err.println("Erro ao executar PUT: " + e.getMessage());
        }
    }

    public void executarGET() {
        Scanner scanner = new Scanner(System.in);

        System.out.print("Digite a key: ");
        String key = scanner.nextLine();

        long timestampCliente = timestamps.getOrDefault(key, 0L);

        int servidorEscolhido = random.nextInt(3);
        String ip = servidorIPs[servidorEscolhido];
        int porta = servidorPortas[servidorEscolhido];

        try (Socket socket = new Socket(ip, porta);
                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {

            // CORREÇÃO: Incluir informações de callback na mensagem GET
            Mensagem getMsg = new Mensagem(Mensagem.TipoMensagem.GET, key, timestampCliente);
            getMsg.setClienteIP("127.0.0.1"); // IP do cliente para callback
            getMsg.setClientePorta(callbackPorta); // Porta do servidor de callback do cliente
            out.writeObject(getMsg);

            Mensagem resposta = (Mensagem) in.readObject();

            if (resposta.getTipo() == Mensagem.TipoMensagem.WAIT_FOR_RESPONSE) {
                System.out.println("GET key: " + key + 
                                 " obtido do servidor " + ip + ":" + porta +
                                 ", meu timestamp " + timestampCliente +
                                 " e do servidor é menor, portanto devolvendo WAIT_FOR_RESPONSE");
                System.out.println("Aguardando resposta assíncrona...");
                
            } else if (resposta.getTipo() == Mensagem.TipoMensagem.GET_RESPONSE) {
                timestamps.put(key, resposta.getTimestamp());
                System.out.println("GET key: " + key + " value: " + resposta.getValue() +
                        " obtido do servidor " + ip + ":" + porta +
                        ", meu timestamp " + timestampCliente +
                        " e do servidor " + resposta.getTimestamp());
            }

        } catch (Exception e) {
            System.err.println("Erro ao executar GET: " + e.getMessage());
        }
    }

    public void executar() {
        Scanner scanner = new Scanner(System.in);

        while (true) {
            System.out.println("\n=== MENU CLIENTE ===");
            System.out.println("1. INIT");
            System.out.println("2. PUT");
            System.out.println("3. GET");
            System.out.println("4. SAIR");
            System.out.print("Escolha uma opção: ");

            String opcao = scanner.nextLine();

            switch (opcao) {
                case "1":
                    inicializar();
                    break;
                case "2":
                    if (servidorIPs[0] == null) {
                        System.out.println("Execute INIT primeiro!");
                        break;
                    }
                    executarPUT();
                    break;
                case "3":
                    if (servidorIPs[0] == null) {
                        System.out.println("Execute INIT primeiro!");
                        break;
                    }
                    if (callbackServer == null) {
                        System.out.println("Execute INIT primeiro para configurar callback!");
                        break;
                    }
                    executarGET();
                    break;
                case "4":
                    System.out.println("Saindo...");
                    try {
                        if (callbackServer != null && !callbackServer.isClosed()) {
                            callbackServer.close();
                        }
                        threadPool.shutdown();
                    } catch (Exception e) {
                        // Ignorar erro de fechamento
                    }
                    return;
                default:
                    System.out.println("Opção inválida!");
            }
        }
    }

    public static void main(String[] args) {
        Cliente cliente = new Cliente();
        cliente.executar();
    }
}