# send_mail.py
import argparse, smtplib, ssl, os, mimetypes
from email.message import EmailMessage

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--smtp-host", required=True)
    p.add_argument("--smtp-port", type=int, default=587)
    p.add_argument("--smtp-user", required=True)
    p.add_argument("--smtp-pass", required=True)
    p.add_argument("--from", dest="from_addr", required=True)
    p.add_argument("--to", required=True)
    p.add_argument("--subject", required=True)
    p.add_argument("--body", default="")
    p.add_argument("--body-file", default="", help="Chemin d'un fichier texte a utiliser comme corps du mail")
    p.add_argument("--attach", default="", help="Chemin d'un fichier a joindre (optionnel)")
    args = p.parse_args()

    body = args.body
    if args.body_file and os.path.exists(args.body_file):
        with open(args.body_file, "r", encoding="utf-8", errors="replace") as f:
            body = f.read()

    msg = EmailMessage()
    msg["From"] = args.from_addr
    msg["To"] = args.to
    msg["Subject"] = args.subject
    msg.set_content(body or "(corps vide)")

    if args.attach and os.path.exists(args.attach):
        ctype, _ = mimetypes.guess_type(args.attach)
        maintype, subtype = (ctype or "application/octet-stream").split("/", 1)
        with open(args.attach, "rb") as f:
            msg.add_attachment(f.read(), maintype=maintype, subtype=subtype,
                               filename=os.path.basename(args.attach))

    ctx = ssl.create_default_context()
    with smtplib.SMTP(args.smtp_host, args.smtp_port) as s:
        s.starttls(context=ctx)
        s.login(args.smtp_user, args.smtp_pass)
        s.send_message(msg)

if __name__ == "__main__":
    main()
