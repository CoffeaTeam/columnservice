from cryptography import x509
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID, ExtendedKeyUsageOID
import datetime
import os.path


COMMON_SUBJECT_ATTRIB = [
    x509.NameAttribute(NameOID.DOMAIN_COMPONENT, "gov"),
    x509.NameAttribute(NameOID.DOMAIN_COMPONENT, "fnal"),
    x509.NameAttribute(NameOID.ORGANIZATIONAL_UNIT_NAME, "Coffea farm"),
]


def generate_ca(common_names):
    private_key = rsa.generate_private_key(
        public_exponent=65537, key_size=2048, backend=default_backend()
    )
    name = x509.Name(
        COMMON_SUBJECT_ATTRIB
        + [x509.NameAttribute(NameOID.COMMON_NAME, name) for name in common_names]
    )
    certificate = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .not_valid_before(datetime.datetime.today() - datetime.timedelta(days=1))
        .not_valid_after(datetime.datetime.today() + datetime.timedelta(days=365))
        .serial_number(x509.random_serial_number())
        .public_key(private_key.public_key())
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(
            private_key=private_key,
            algorithm=hashes.SHA256(),
            backend=default_backend(),
        )
    )
    return certificate, private_key


def generate_server_cert(ca_cert, ca_key, common_names):
    private_key = rsa.generate_private_key(
        public_exponent=65537, key_size=2048, backend=default_backend()
    )
    name = x509.Name(
        COMMON_SUBJECT_ATTRIB
        + [x509.NameAttribute(NameOID.COMMON_NAME, name) for name in common_names]
    )
    certificate = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(ca_cert.subject)
        .not_valid_before(datetime.datetime.today() - datetime.timedelta(days=1))
        .not_valid_after(datetime.datetime.today() + datetime.timedelta(days=365))
        .serial_number(x509.random_serial_number())
        .public_key(private_key.public_key())
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                content_commitment=False,
                key_encipherment=True,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=False,
                crl_sign=False,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(
            x509.ExtendedKeyUsage(
                [ExtendedKeyUsageOID.CLIENT_AUTH, ExtendedKeyUsageOID.SERVER_AUTH]
            ),
            critical=True,
        )
        .sign(private_key=ca_key, algorithm=hashes.SHA256(), backend=default_backend())
    )
    return certificate, private_key


def generate_csr(common_names):
    private_key = rsa.generate_private_key(
        public_exponent=65537, key_size=2048, backend=default_backend()
    )
    name = x509.Name(
        COMMON_SUBJECT_ATTRIB
        + [x509.NameAttribute(NameOID.COMMON_NAME, name) for name in common_names]
    )
    csr = (
        x509.CertificateSigningRequestBuilder()
        .subject_name(name)
        .sign(
            private_key=private_key,
            algorithm=hashes.SHA256(),
            backend=default_backend(),
        )
    )
    return csr, private_key


def sign_csr(ca_cert, ca_key, csr, valid_until=None):
    if not csr.is_signature_valid:
        raise ValueError("CSR has an invalid signature, not signing!")
    if len(csr.extensions) > 0:
        raise ValueError("CSR has extensions, we forbid this for simplicity")
    if valid_until is None:
        valid_until = datetime.datetime.now() + datetime.timedelta(hours=2)
    cb = (
        x509.CertificateBuilder()
        .subject_name(csr.subject)
        .issuer_name(ca_cert.subject)
        .not_valid_before(datetime.datetime.now())
        .not_valid_after(valid_until)
        .serial_number(x509.random_serial_number())
        .public_key(csr.public_key())
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                content_commitment=False,
                key_encipherment=True,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=False,
                crl_sign=False,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(
            x509.ExtendedKeyUsage([ExtendedKeyUsageOID.CLIENT_AUTH]), critical=True,
        )
    )
    # for extension in csr.extensions:
    #     cb = cb.add_extension(extension.value, critical=extension.critical)
    certificate = cb.sign(
        private_key=ca_key, algorithm=hashes.SHA256(), backend=default_backend()
    )
    return certificate


def write_secrets(prefix):
    ca_cert, ca_key = generate_ca(["Coffea farm CA"])
    with open(os.path.join(prefix, "ca.key"), "wb") as f:
        f.write(
            ca_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.TraditionalOpenSSL,
                encryption_algorithm=serialization.BestAvailableEncryption(b"bananas"),
            )
        )
    with open(os.path.join(prefix, "ca.crt"), "wb") as f:
        f.write(ca_cert.public_bytes(encoding=serialization.Encoding.PEM,))

    server_cert, server_key = generate_server_cert(
        ca_cert, ca_key, ["Coffea dask cluster"]
    )
    with open(os.path.join(prefix, "hostcert.pem"), "wb") as f:
        f.write(
            server_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.TraditionalOpenSSL,
                encryption_algorithm=serialization.NoEncryption(),
            )
        )
        f.write(server_cert.public_bytes(encoding=serialization.Encoding.PEM,))


def create_user_cert(ca_prefix, username, fullname, output_buffer):
    user_csr, user_key = generate_csr([username, fullname])
    with open(os.path.join(ca_prefix, "ca.crt"), "rb") as fin:
        ca_cert = x509.load_pem_x509_certificate(fin.read(), default_backend())
    with open(os.path.join(ca_prefix, "ca.key"), "rb") as fin:
        ca_key = serialization.load_pem_private_key(
            fin.read(), b"bananas", default_backend()
        )
    user_cert = sign_csr(ca_cert, ca_key, user_csr)
    output_buffer.write(
        user_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption(),
        )
    )
    output_buffer.write(user_cert.public_bytes(encoding=serialization.Encoding.PEM,))
    return output_buffer


if __name__ == "__main__":
    write_secrets(".")
