from py4j.java_gateway import JavaGateway, GatewayParameters, CallbackServerParameters


class JavaGateWay(object):
    def __init__(self):
        self.gateway = JavaGateway(auto_convert=True,
                                   gateway_parameters=GatewayParameters(auto_convert=True))

    def get_gate_way(self):
        return self.gateway

    def get_jvm(self):
        return self.gateway.jvm

    def get_java_entry(self):
        return self.gateway.entry_point
