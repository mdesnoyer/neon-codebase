"""Collection of email templates

A function will return a tuple (subject, body).
"""

def verify():
    return (
"Welcome to Neon For Videos",
"""<html>
  <body>
    <p><a href="https://www.neon-lab.com">
    <img class="logo"
     style = "height: 43.25px; width: 104px;"
     src="https://s3.amazonaws.com/neon_website_assets/logo_777.png"
     alt="Neon">
    </a></p>
    <br>
    <p>Hi {first_name},</p>
    <p>Thank you for signing up for Neon for Video.
       You're just a step away from getting more
       value from your videos. First, please verify
       your account by clicking here:
       <a style="color: #f16122" href="{url}">
       verify your account
       </a>.
    </p>
    <p>For your reference, your username is:
       <a href="" style="text-decoration:none !important; color:#000000 !important;">{username}</a>
    </p>
    <p>
      <a style="color: #f16122" href="https://app.neon-lab.com/signin">
      Login </a> to your account.
    </p>
    <p>Thanks,<br>
      The Neon Team
    </p>
    <p style="font-size:smaller">
       If the verification link does not work please copy
       and paste the following address into your
       browser : {url}
    </p>
    <p>------------</p>
    <p class="footer" style="font-size: smaller">Neon Labs Inc.<br>
    70 South Park St. | San Francisco, CA 94107<br>
    (415) 355-4249<br>
    <a style="color: #f16122" href="https://www.neon-lab.com">neon-lab.com</a>
    </p>
    <p class="footer" style="font-size: smaller">This is an automated email.
    Please get in touch with us at
    <a href="mailto:ask@neon-lab.com">
      ask@neon-lab.com</a></p>
  </body>
</html>""")
