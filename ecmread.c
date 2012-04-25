#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <errno.h>
#include <limits.h>
#include <fcntl.h>
#include <time.h>
#include <stdarg.h>

#ifndef MIN
#define MIN(X,Y) ((X) < (Y) ? (X) : (Y))
#endif
#ifndef MAX
#define MAX(X,Y) ((X) > (Y) ? (X) : (Y))
#endif

#define TRUE 1
#define FALSE 0

volatile uint8_t running = TRUE;

struct Program;

enum
{
  START_HEADER0    = 254,
  START_HEADER1    = 255,
  ECM1240_PACKET_ID = 3,
  END_HEADER0       = 255,
  END_HEADER1       = 254,
  DATA_BYTES_LENGTH = 59,             // does not include the start/end headers
  TOTAL_PACKET_LEN  = DATA_BYTES_LENGTH + 6,
  SEC_COUNTER_MAX   = 16777216,
  ECM1240_UNIT_ID   = 3
};

struct Buffer
{
  char * raw_buffer;
  size_t raw_buffer_size;

  char * buffer;
  size_t buffer_len;
};
  
struct Timer;

typedef enum 
{
  BIDIRECTIONAL,
  UNIDIRECTIONAL,
  RELAY
} Socket_Type;

typedef enum
{
  CRIT,
  ERROR,
  WARN,
  INFO,
  DEBUG
} LogLevel;

struct Socket
{
  struct sockaddr_in sin;
  int fd;
  struct Connection * connection;
  uint8_t wants_read;
  uint8_t wants_write;
  void (*callback)(struct Program *, struct Socket * socket, uint8_t, uint8_t);
  struct Buffer * read_buffer;
  struct Buffer * write_buffer;
  struct Timer * timer;
  Socket_Type socket_type;
  struct Socket * next;
};

struct Connection
{
  char * name;
  char * host;
  uint16_t port;
  uint8_t connector;
  uint16_t connect_delay;
  uint8_t controller;
  struct Connection * next;
  struct Socket * socket;
};

struct Config
{
  struct Connection input;
  struct Connection connections;
  LogLevel level;
};


struct Timer
{
  time_t timeout;
  void (*callback)(struct Program *, void * data);
  void * data;
  struct Timer * next;
};

struct Program
{
  struct Config config;
  struct Socket sockets;
  struct Timer timers;
  time_t min_timeout;
  int min_socket;
  int max_socket;
};


void init_config(struct Config * config)
{
  memset(config, 0, sizeof(struct Config));
};

void new_socket(struct Socket ** socket)
{
  *socket = malloc(sizeof(struct Socket));
}

void init_socket(struct Socket * socket)
{
  memset(socket, 0, sizeof(struct Socket));
}

void new_connection(struct Connection ** connection)
{
  *connection = malloc(sizeof(struct Connection));
}

void init_connection(struct Connection * connection)
{
  memset(connection, 0, sizeof(struct Connection));
}

void new_timer(struct Timer ** timer)
{
  *timer = malloc(sizeof(struct Timer));
}

void init_timer(struct Timer * timer)
{
  memset(timer, 0, sizeof(struct Timer));
}

void init_program(struct Program * program)
{
  init_config(&program->config);
  init_socket(&program->sockets);
  init_timer(&program->timers);
  program->min_timeout = 0;
}


void destroy_timer(struct Timer * timer)
{
  free(timer);
}

void trim(char const * str, char const **begin, char const ** end)
{
  *begin = str;
  while (**begin != '\0' && (**begin == ' ' || **begin == '\t'))
  {
    ++(*begin);
  }
  *end = *begin + strlen(*begin);
  while (*end != *begin  && (**end == ' ' || **end == '\t'))
  {
    --(*end);
  }
}
      
void trim_copy(char const * p, size_t len, char * dst)
{
  while ((*p == ' ' || *p == '\t') && len > 0)
  {
    p++;
    len--;
  }

  strncpy(dst, p, len);
  dst[len] = 0;
  char * tmp = dst + strlen(dst) - 1;
  
  while ((*tmp == ' ' || *tmp == '\t') && tmp != dst)
  {
    *tmp = '\0';
  }
}

struct Program * program__;

void putlog(LogLevel level, char const * format, ...)
{
  if (program__->config.level >= level)
  {
    va_list argp;
    va_start(argp, format);
    vprintf(format, argp);
    va_end(argp);
  }
}
      
struct Timer * start_timer(struct Program * program, size_t timeout, void (*callback)(struct Program *, void *), void * data)
{
  struct Timer * timer=&program->timers;
  while (timer->next != NULL)
  {
    timer = timer->next;
  }
  new_timer(&timer->next);
  timer = timer->next;
  init_timer(timer);
  putlog(DEBUG, "Setting time out for %d seconds from now\n", timeout);
  timer->timeout = time(NULL) + timeout;
  timer->callback = callback;
  timer->data = data;
  program->min_timeout = MIN(program->min_timeout, timer->timeout);
  if (program->min_timeout == 0)
  {
    program->min_timeout = timer->timeout;
  }
  return timer;
}

void stop_timer(struct Program * program, struct Timer * timer)
{
  struct Timer * prev=&program->timers;
  while (prev->next != timer)
  {
    prev = prev->next;
  }
  prev->next = timer->next;
  destroy_timer(timer);
}

void destroy_socket(struct Socket * socket)
{
  free(socket);
}

void close_socket(struct Program * program, struct Socket * socket)
{
  struct Socket * prev = &program->sockets;
  while (prev->next != socket)
  {
    prev = prev->next;
  }
  prev->next = socket->next;
  close(socket->fd);
  if (socket->connection)
  {
    socket->connection->socket = NULL;
  }
  destroy_socket(socket);
}

struct Socket * link_new_socket(struct Program * program)
{
  struct Socket * socket = &program->sockets;
  while (socket->next != NULL)
  {
    socket = socket->next;
  }
  new_socket(&socket->next);
  socket = socket->next;
  init_socket(socket);
  return socket;
}

int read_config(FILE * f, struct Config * config, char * error, size_t error_len)
{
  char buffer[1024];
  struct Connection * current_connection = &config->connections;
  uint32_t line=0;
  while (++line, fgets(buffer, sizeof(buffer) - 1, f) != NULL)
  {
    size_t buf_len = strlen(buffer);
    if (buffer[buf_len - 1] == '\n')
    {
      buffer[buf_len - 1] = '\0';
    }
    char const * begin;
    char const * end;
    trim(buffer, &begin, &end);
    if (*begin == '#')
    {
      continue;
    }
    else if (*buffer == '\0')
    {
      continue;
    }
    else if (*begin == '[' && *(end - 1) == ']')
    {
      if (strncasecmp(begin + 1, "input", end - begin - 2) == 0)
      {
        current_connection = &config->input;
        init_connection(current_connection);
        current_connection->name = strdup("INPUT");
      }
      else
      {
        char * p = strchr(begin + 1, '/');
        if (!p)
        {
          snprintf(error, error_len, "Unknown section heading '%s' format should be [Type/Name] line=%u", buffer, line);
          return -1;
        }
        char section[1024];
        strncpy(section, begin + 1, p - (begin + 1));
        section[p - begin - 1] = 0;
        if (strcasecmp(section, "connection") == 0)
        {
          new_connection(&current_connection->next);
          current_connection = current_connection->next;
          init_connection(current_connection);
          char tmp[1024];
          strncpy(tmp, p + 1, (end - 1) - (p + 1));
          tmp[(end - 1) - (p + 1)] = 0;
          current_connection->name = strdup(tmp);
        }
        else
        {
          snprintf(error, error_len, "Unknown type %s on heading '%s' line=%u", section, buffer, line);
          return -1;
        }
      }
    }
    else
    {
      char * p = strchr(begin, '=');
      if (!p)
      {
        snprintf(error, error_len, "Unknown line '%s' line=%u", buffer, line);
        return -1;
      }
      char key[1024];
      char value[1024];
      trim_copy(begin, p - begin, key);
      trim_copy(p + 1, end - (p + 1), value);
      if (strcasecmp(key, "connector") == 0)
      {
        current_connection->connector = atoi(value);
      }
      else if (strcasecmp(key, "controller") == 0)
      {
        current_connection->controller = atoi(value);
      }
      else if (strcasecmp(key, "connect_delay") == 0)
      {
        current_connection->connect_delay = atoi(value);
      }
      else if (strcasecmp(key, "port") == 0)
      {
        current_connection->port = atoi(value);
      }
      else if (strcasecmp(key, "host") == 0)
      {
        current_connection->host = strdup(value);
      }
      else if (strcasecmp(key, "loglevel") == 0)
      {
        if (strcasecmp(value, "crit") == 0)
        {
          config->level = CRIT;
        }
        else if (strcasecmp(value, "warn") == 0)
        {
          config->level = WARN;
        }
        else if (strcasecmp(value, "info") == 0)
        {
          config->level = INFO;
        }
        else if (strcasecmp(value, "debug") == 0)
        {
          config->level = DEBUG;
        }
      }
      else
      {
        snprintf(error, error_len, "Unknown key (%s=%s) (%s) line=%u\n", key, value, buffer, line);
        return -1;
      }
    }
  }
  return 0;
}


void print_conn(struct Connection const * conn)
{
  printf("Name=%s\n", conn->name);
  printf("Host=%s\n", conn->host);
  printf("Port=%u\n", conn->port);
  printf("Is connector?: %s\n", conn->connector ? "true" : "false");
  printf("Connect delay: %u\n", conn->connect_delay);
  printf("Controller?: %s", conn->controller ? "true" : "false");
}

void print_config(struct Config const * config)
{
  struct Connection const * conn= &config->connections;
  conn = conn->next;
  printf("Input:\n");
  print_conn(&config->input);
  printf("\n");
  printf("\n");

  for (; conn != NULL; conn = conn->next)
  {
    print_conn(conn);
    printf("\n");
    printf("\n");
  }
  printf("\n");
  printf("\n");
}


void send_to_all(struct Program * program, char const * buffer, size_t buffer_len, Socket_Type type);

void write_buffer_to_socket(struct Socket * socket)
{
  //NOOP
}

uint8_t check_byte(size_t byte, unsigned char c, char ** buffer, size_t * length)
{
  unsigned char c1;
  c1 = (*((unsigned char **)buffer))[byte];
  if (c1 == c)
  {
    return TRUE;
  }
  *buffer += (byte + 1);
  *length -= (byte + 1);
  putlog(DEBUG, "Unexpected byte %u at pos %lu, expected %u\n", c1, byte, c);
  return FALSE;
}
    
void check_buffer_for_packets(struct Program * program, struct Socket * socket)
{
  struct Buffer * buffer = socket->read_buffer;
  putlog(DEBUG, "Checking buffer of size: %lu\n", buffer->buffer_len);
  while (buffer->buffer_len >= TOTAL_PACKET_LEN)
  {
    if (!check_byte(0, START_HEADER0, &buffer->buffer, &buffer->buffer_len)) { continue; }
    if (!check_byte(1, START_HEADER1, &buffer->buffer, &buffer->buffer_len))  { continue; }
    if (!check_byte(2, ECM1240_PACKET_ID, &buffer->buffer, &buffer->buffer_len)) { continue; }
    if (!check_byte(3 + DATA_BYTES_LENGTH, END_HEADER0, &buffer->buffer, &buffer->buffer_len)) { continue; } 
    if (!check_byte(4 + DATA_BYTES_LENGTH, END_HEADER1, &buffer->buffer, &buffer->buffer_len))  { continue; }

    unsigned char * b = (unsigned char *)buffer->buffer;
    
    if (b[3 + 29] != ECM1240_UNIT_ID)
    {
      putlog(DEBUG, "Unexpected unit id: %u\n", b[3 + 29]);
      goto end;
    }

    size_t checksum=0;
    size_t i;
    for (i=0; i <= 63; ++i)
    {
      checksum += b[i];
    }
    checksum &= 0xFF;
    if (b[64] != checksum)
    {
      putlog(DEBUG, "Bad checksum for packet: got %u expected %lu\n", b[64], checksum);
      goto end;
    }

    putlog(DEBUG, "Got valid packet\n");
    send_to_all(program, buffer->buffer, TOTAL_PACKET_LEN, UNIDIRECTIONAL);
end:
    buffer->buffer_len -= TOTAL_PACKET_LEN;
    buffer->buffer += TOTAL_PACKET_LEN;
  }
}

void send_to_input(struct Program *, char * buffer, size_t buffer_len);
void configure_socket(
    struct Program * program, 
    struct Socket * socket, 
    int fd, 
    struct Connection * connection, 
    struct Connection * connected_to,
    uint8_t connected);
int setup_connection(struct Program * program, struct Connection * connection);
void do_reconnect(struct Program * program, void * data);
void reconnect_socket(struct Program * program, struct Socket * socket);

void relay_data(struct Program * program, struct Socket * socket, uint8_t do_read, uint8_t do_write)
{
  if (do_read)
  {
    char buffer[1024];
    int ret = read(socket->fd, buffer, sizeof(buffer));
    if ((ret < 0 && ret != EINTR) || ret == 0)
    {
      putlog(WARN, "Socket error %d - (%d)%s\n", socket->fd, errno, strerror(errno));
      reconnect_socket(program, socket);
      return;
    }
    putlog(DEBUG, "Read %d bytes from socket\n", ret);
    // Controllers get raw feed as it comes in, non-controllers will only get valid data packets as a whole
    send_to_all(program, buffer, ret, BIDIRECTIONAL);
    if (((socket->read_buffer->buffer - socket->read_buffer->raw_buffer) + socket->read_buffer->buffer_len + ret) > socket->read_buffer->raw_buffer_size)
    {
      putlog(DEBUG, "Moving buffer down, it's too large\n");
      memmove(socket->read_buffer->raw_buffer, socket->read_buffer->buffer, socket->read_buffer->buffer_len);
      socket->read_buffer->buffer = socket->read_buffer->raw_buffer;
    }
    memcpy(socket->read_buffer->buffer + socket->read_buffer->buffer_len, buffer, ret);
    socket->read_buffer->buffer_len += ret;
    check_buffer_for_packets(program, socket);
  }
  if (do_write)
  {
    write_buffer_to_socket(socket);
  }
}


void client_connection(struct Program * program, struct Socket * socket, uint8_t do_read, uint8_t do_write)
{
  if (do_read)
  {
    char buffer[65 * 1024];
    int ret = read(socket->fd, buffer, sizeof(buffer));
    if ((ret < 0 && ret != EINTR) || ret == 0)
    {
      putlog(WARN, "Socket error %d - (%d)%s\n", socket->fd, errno, strerror(errno));
      reconnect_socket(program, socket);
      return;
    }
    if (socket->socket_type == BIDIRECTIONAL)
    {
      send_to_input(program, buffer, ret);
    }
  }
  if (do_write)
  {
    write_buffer_to_socket(socket);
  }
}

void send_to_input(struct Program * program, char * buffer, size_t buffer_len)
{
  struct Connection * connection = &program->config.input;
  struct Socket * socket=NULL;
  if (!connection->connector)
  {
    socket = program->sockets.next;
    while (socket != NULL)
    {
      if (socket->callback == relay_data)
      {
        break;
      }
      socket = socket->next;
    }
  }
  else
  {
    socket = connection->socket;
  }
  if (socket != NULL)
  {
    write(socket->fd, buffer, buffer_len);
  }
}

void send_to_all(struct Program * program, char const * buffer, size_t buffer_len, Socket_Type type)
{
  putlog(DEBUG, "Sending data to all %lu\n", buffer_len);
  struct Socket * socket = program->sockets.next;
  while (socket != NULL)
  {
    if (socket->socket_type == type)
    {
      //TODO: hacks
      if (socket->callback != client_connection)
      {
        goto next;
      }
      putlog(DEBUG, "Sending to socket %d\n", socket->fd);
      int ret = write(socket->fd, (void *)buffer, buffer_len);
      if (ret != buffer_len)
      {
        putlog (WARN, "Write returned: %d\n", ret);
      }
      if (ret == -1)
      {
        putlog(WARN, "Error: %s\n", strerror(errno));
      }
    }
next:
    socket = socket->next;
  }
}


void wait_for_connection(struct Program * program, struct Socket * socket, uint8_t do_read, uint8_t do_write)
{
  stop_timer(program, socket->timer);
  int n = connect(socket->fd, (struct sockaddr *)&socket->sin, sizeof(socket->sin));
  if (n < 0)
  {
    if (errno == EINPROGRESS)
    {
      return;
    }
    if (errno != EISCONN)
    {
      putlog(WARN, "Socket error on fd: %d - %s\n", socket->fd, strerror(errno));
      reconnect_socket(program, socket);
      return;
    }
  }
  int error=0;
  socklen_t slen=sizeof(int);
  if (getsockopt(socket->fd, SOL_SOCKET, SO_ERROR, &error, &slen) < 0)
  {
    putlog(WARN, "Socket (SO_ERROR) error on fd -: %d - %s\n", socket->fd, strerror(errno));
    reconnect_socket(program, socket);
    return;
  }
  if (error != 0)
  {
    putlog(WARN, "Socket error on fd (error=%d): %d - %s - %s\n", error, socket->fd, strerror(errno), strerror(error));
    reconnect_socket(program, socket);
    return;
  }
  putlog(DEBUG, "Do_read = %d do_write = %d\n", do_read, do_write);
  putlog(INFO, "Connection accepted! %d\n", socket->fd);
  socket->wants_write = FALSE;
  socket->callback = client_connection;
}


void accept_connection(struct Program * program, struct Socket * socket, uint8_t read, uint8_t write)
{
  struct sockaddr_in addr;
  int len=sizeof(struct sockaddr_in);
  int fd = accept(socket->fd, (struct sockaddr *) &addr, (socklen_t  *)&len);
  if (fd < 0)
  {
    return;
  }
  struct Socket * new_socket = link_new_socket(program);
  fcntl(fd, F_SETFL, fcntl(fd, F_GETFL) | O_NONBLOCK);
  configure_socket(program, new_socket, fd, socket->connection, NULL, TRUE);
}


void do_reconnect(struct Program * program, void * data)
{
  struct Connection * connection = (struct Connection *)data;
  putlog(INFO, "Reconnecting to socket...\n");
  setup_connection(program, connection);
}

void reconnect_socket(struct Program * program, struct Socket * socket)
{
  if (socket->connection && socket->connection->connector)
  {
    putlog(DEBUG, "Starting timer....\n");
    start_timer(program, socket->connection->connect_delay, do_reconnect, socket->connection);
  }
  close_socket(program, socket);
}

void cancel_connection(struct Program * program, void * data)
{
  struct Socket * socket = (struct Socket *)data;
  putlog(DEBUG, "Timeout making connection %d\n", socket->fd);
  reconnect_socket(program, socket);
}

void configure_socket(
    struct Program * program, 
    struct Socket * new_socket, 
    int fd, 
    struct Connection * connection, 
    struct Connection * connected_to,
    uint8_t connected)
{
  new_socket->fd = fd;
  new_socket->connection = connection;
  new_socket->wants_read = TRUE;
  new_socket->wants_write = FALSE;
  if (connected_to)
  {
    connected_to->socket = new_socket;
    connected_to->socket->connection = connected_to;
  }
    
  if (connection == &program->config.input)
  {
    new_socket->socket_type = RELAY;
  }
  else
  {
    new_socket->socket_type = connection->controller ? BIDIRECTIONAL : UNIDIRECTIONAL;
  }

  if (new_socket->socket_type == RELAY &&
      (connection->connector || (!connection->controller && !connected_to)))
  {
    putlog(DEBUG, "Allocating relay buffer...\n");
    new_socket->callback = relay_data;
    new_socket->read_buffer = malloc(sizeof(struct Buffer));
    const size_t BUFFER_SIZE=8 * 1024;
    new_socket->read_buffer->raw_buffer = malloc(BUFFER_SIZE);
    new_socket->read_buffer->raw_buffer_size = BUFFER_SIZE;
    new_socket->read_buffer->buffer = new_socket->read_buffer->raw_buffer;
    new_socket->read_buffer->buffer_len = 0;
  }

  putlog(DEBUG, "Adding socket: %d\n", fd);
  if (connected)
  {
    if (connection->connector)
    {
      new_socket->callback = new_socket->socket_type == RELAY ? relay_data : client_connection;
    }
    else
    {
      new_socket->callback = connected_to ? accept_connection : (new_socket->socket_type == RELAY ? relay_data : client_connection);
    }
  }
  /* This should only happen for outbound connections */
  else
  {
    putlog(DEBUG, "Socket needs to wait for connect\n");
    new_socket->wants_write = TRUE;
    new_socket->callback = wait_for_connection;
    //TODO: configurable timer
    new_socket->timer = start_timer(program, 5U, cancel_connection, new_socket);
  }
}

int setup_connection(struct Program * program, struct Connection * connection)
{
  if (connection->connector)
  {
    putlog(DEBUG, "Making accept socket\n");
    struct sockaddr_in sin;
    memset(&sin, 0, sizeof(sin));
    sin.sin_family = AF_INET;
    sin.sin_port = htons(connection->port);
    sin.sin_addr.s_addr = inet_addr(connection->host);
    int fd = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP);
    fcntl(fd, F_SETFL, fcntl(fd, F_GETFL) | O_NONBLOCK);
    int n = connect(fd, (struct sockaddr *)&sin, sizeof(sin));
    if (n < 0)
    {
      if (errno != EINPROGRESS)
      {
        close(fd);
        return -1;
      }
    }
    struct Socket * new_socket = link_new_socket(program);
    uint8_t connected = !n;
    configure_socket(program, new_socket, fd, connection, connection, connected);
    memcpy(&new_socket->sin, &sin, sizeof(sin));
  }
  else
  {
    putlog(DEBUG, "Making accept socket\n");
    struct sockaddr_in sin;
    memset(&sin, 0, sizeof(struct sockaddr_in));
    sin.sin_family = AF_INET;
    sin.sin_addr.s_addr = inet_addr(connection->host);
    sin.sin_port = htons(connection->port);

    int fd=-1;
    int type;
    type = SOCK_STREAM;
    if ((fd = socket(PF_INET, type, IPPROTO_TCP)) < 0)
    {
      goto error;
    }
    int On = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &On, sizeof(On));
    if( bind(fd, (struct sockaddr *)&sin, sizeof(sin)) < 0 )
    {
      goto error;
    }
    if (listen(fd, 5) < 0)
    {
      goto error;
    }
    struct Socket * new_socket = link_new_socket(program);
    configure_socket(program, new_socket, fd, connection, connection, TRUE);
    putlog(DEBUG, "Adding socket: %d\n", fd);
    return 0;
error:
    putlog(DEBUG, "Error %s\n", strerror(errno));
    if (fd >=0)
    {
      close(fd);
      return -1;
    }
  }
  return -1;
}

void initial_setup(struct Program * program)
{
  setup_connection(program, &program->config.input);

  struct Connection * connection;
  for (connection = program->config.connections.next; connection != NULL; connection = connection->next)
  {
    setup_connection(program, connection);
  }
}

void main_loop(struct Program * program)
{
  while (running)
  {
    fd_set readset;
    FD_ZERO(&readset);
    fd_set writeset;
    FD_ZERO(&writeset);

    int max_socket = 0;
    struct Socket * socket = program->sockets.next;
    while (socket != NULL)
    {
      putlog(DEBUG, "Looking at active socket %d\n", socket->fd);
      if (socket->wants_read)
      {
        putlog(DEBUG, "Adding read\n");
        FD_SET(socket->fd, &readset);
      }
      if (socket->wants_write)
      {
        putlog(DEBUG, "Adding write\n");
        FD_SET(socket->fd, &writeset);
      }
      max_socket = MAX(socket->fd, max_socket);
      socket = socket->next;
    }
    putlog(DEBUG, "Done\n");
    int ret;

    time_t timeout=program->min_timeout;

    struct timeval * tv_p=NULL;
    struct timeval tv;

    if (timeout > 0)
    {
      tv.tv_usec = 0;
      tv.tv_sec = timeout - time(NULL);
      tv_p = &tv;
    }

    while ((ret = select(max_socket + 1, &readset, &writeset, NULL, tv_p)) == EAGAIN) { }
    putlog(DEBUG, "Done with select\n");
    if (tv_p != NULL)
    {
      time_t now = time(NULL);
      if (now >= program->min_timeout)
      {
        program->min_timeout = 0;
        struct Timer * timer = program->timers.next;
        struct Timer * prev_timer = &program->timers;
        while (timer != NULL)
        {
          if (now >= timer->timeout)
          {
            timer->callback(program, timer->data);
            struct Timer * tmp = timer;
            prev_timer->next = timer->next;
            timer = timer->next;
            destroy_timer(tmp);
          }
          else
          {
            if (program->min_timeout == 0)
            {
              program->min_timeout = timer->timeout;
            }
            program->min_timeout = MIN(program->min_timeout, timer->timeout);
            prev_timer = timer;
            timer = timer->next;
          }
        }
      }
    }
    if (ret != 0)
    {
      socket = program->sockets.next;
      while (socket != NULL)
      {
        struct Socket * calling_socket = socket;
        socket = socket->next;
        uint8_t r=FD_ISSET(calling_socket->fd, &readset);
        uint8_t w=FD_ISSET(calling_socket->fd, &writeset);
        if (r || w)
        {
          putlog(DEBUG, "Socket: %d - %d - %d\n\n", calling_socket->fd, r, w);
          calling_socket->callback(program, calling_socket, r, w);
        }
      }
    }
    putlog(DEBUG, "Done\n");
  }
}

int main(int argc, char ** argv)
{
  struct Program program;
  program__ = &program;
  if (argc < 2)
  {
    printf("Usage %s <file>\n", argv[0]);
    exit(1);
  }
  init_program(&program);
  FILE * f = fopen(argv[1], "r");
  if (!f)
  {
    printf("Unable to open file\n");
    exit(1);
  }
  char buffer[1024];
  if (read_config(f, &program.config, buffer, sizeof(buffer)) < 0)
  {
    printf("Error: %s", buffer);
    exit(1);
  }
  print_config(&program.config);
  initial_setup(&program);
  main_loop(&program);
  return 0;
}
