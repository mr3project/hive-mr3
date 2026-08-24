package com.datamonad.mr3.timeline;

import org.slf4j.LoggerFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hive.http.HttpServer;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.servlet.ServletContext;
import java.security.Principal;

/**
 * Server endpoints: GET /server/mr3/{path}
 */
// prefix "/server" should be omitted because it is the mount path.
@Path("/mr3")
public class ServerResource {

  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(ServerResource.class);

  public ServerResource() {
  }

  @Context
  SecurityContext security;

  @Context
  HttpServletRequest request;

  @Context
  ServletContext servletContext;

  @GET @Path("userInfo") @Produces(MediaType.APPLICATION_JSON)
  public Response userInfo() {
    // Prefer Jersey SecurityContext, fallback to servlet request
    Principal p = (security != null) ? security.getUserPrincipal() : null;
    if (p == null && request != null) {
      p = request.getUserPrincipal();
    }

    // Also try request.getRemoteUser() as fallback
    String username = null;
    if (p != null) {
      username = p.getName();
    } else if (request != null) {
      username = request.getRemoteUser();
    }

    if (username == null || username.trim().isEmpty()) {
      username = "anonymous";
    }

    LOG.debug("ServerResource.userInfo() called by user: {}", username);

    // Check admin status from servlet context
    boolean isAdmin = false;
    if (servletContext != null && !username.equals("anonymous")) {
      Object adminsAttr = servletContext.getAttribute(HttpServer.ADMINS_ACL);
      if (adminsAttr instanceof AccessControlList) {
        isAdmin = ((AccessControlList) adminsAttr).isUserAllowed(
            UserGroupInformation.createRemoteUser(username));
      }
    }

    UserInfo userInfo = new UserInfo(username, isAdmin);

    return Response.ok(userInfo)
        .header(HttpHeaders.CACHE_CONTROL, "no-store, no-cache, must-revalidate")
        .build();
  }

  public static class UserInfo {
    public final String username;
    public final boolean admin;

    public UserInfo(String username, boolean admin) {
      this.username = username;
      this.admin = admin;
    }

    // Optional: Add Jackson annotations if needed
    // @JsonProperty("username") public String getUsername() { return username; }
    // @JsonProperty("roles") public List<String> getRoles() { return roles; }
    // @JsonProperty("admin") public boolean isAdmin() { return admin; }
  }
}
