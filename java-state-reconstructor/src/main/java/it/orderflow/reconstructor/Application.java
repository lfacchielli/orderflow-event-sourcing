package it.orderflow.reconstructor;

public final class Application {

    private Application() {
    }

    public static void main(String[] args) {
        System.out.println("OrderFlow Java State Reconstructor");
        System.out.println("Status: ready");
    }

    public static String applicationName() {
        return "OrderFlow Java State Reconstructor";
    }
}