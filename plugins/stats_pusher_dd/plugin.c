#include <uwsgi.h>

/*

this is a stats pusher plugin for the dd server:

--stats-push dd:address[,prefix]

example:

--stats-push dd:127.0.0.1:8125,myinstance

it exports values exposed by the metric subsystem

*/

extern struct uwsgi_server uwsgi;

// configuration of a dd node
struct dd_node {
  int fd;
  union uwsgi_sockaddr addr;
  socklen_t addr_len;
  char *prefix;
  uint16_t prefix_len;
};

static int dd_generate_tags(char *metric, char *new_metric, char *tags) {
  int metric_id;
  char metric_key[MAX_VARS];
  char metric_name[MAX_VARS];

  // verify if we can find metric ID
  if (sscanf(metric, "%[^.].%d.%[^$]", metric_name, &metric_id, metric_key) == 3) {

    // prepare tags metadata string
    int ret = snprintf(tags, (MAX_VARS - 1), "|#%s:%i", metric_name, metric_id);
    if (ret <= 0 || ret >= (int) (MAX_VARS - 1)) {
      return -1;
    }

    // set the new metric name
    ret = snprintf(new_metric, (MAX_VARS - 1), "%s.%s", metric_name, metric_key);
    if (ret <= 0 || ret >= (int) (MAX_VARS - 1)) {
      return -1;
    }

    return 1;
  }

  return 0;

}

static int dd_send_metric(struct uwsgi_buffer *ub, struct uwsgi_stats_pusher_instance *uspi, char *metric, size_t metric_len, int64_t value, char type[2]) {
  struct dd_node *sn = (struct dd_node *) uspi->data;

  char tags[MAX_VARS];
  char new_metric[MAX_VARS];

  // reset the buffer
  ub->pos = 0;

  int got_tags = dd_generate_tags(metric, new_metric, tags);
  if ( got_tags < 0 ) return -1;

  if (uwsgi_buffer_append(ub, sn->prefix, sn->prefix_len)) return -1;
  if (uwsgi_buffer_append(ub, ".", 1)) return -1;

  // put the new_metric if we found some tags
  if (got_tags) {
    if (uwsgi_buffer_append(ub, new_metric, strlen(new_metric))) return -1;
  } else {
    if (uwsgi_buffer_append(ub, metric, strlen(metric))) return -1;
  }

  if (uwsgi_buffer_append(ub, ":", 1)) return -1;
  if (uwsgi_buffer_num64(ub, value)) return -1;
  if (uwsgi_buffer_append(ub, type, 2)) return -1;

  // add tags metadata if there are any
  if (got_tags) {
    if (uwsgi_buffer_append(ub, tags, strlen(tags))) return -1;
  }

  if (sendto(sn->fd, ub->buf, ub->pos, 0, (struct sockaddr *) &sn->addr.sa_in, sn->addr_len) < 0) {
    uwsgi_error("dd_send_metric()/sendto()");
  }

  return 0;
}


static void stats_pusher_dd(struct uwsgi_stats_pusher_instance *uspi, time_t now, char *json, size_t json_len) {

  if (!uspi->configured) {
    struct dd_node *sn = uwsgi_calloc(sizeof(struct dd_node));
    char *comma = strchr(uspi->arg, ',');
    if (comma) {
      sn->prefix = comma+1;
      sn->prefix_len = strlen(sn->prefix);
      *comma = 0;
    }
    else {
      sn->prefix = "uwsgi";
      sn->prefix_len = 5;
    }

    char *colon = strchr(uspi->arg, ':');
    if (!colon) {
      uwsgi_log("invalid dd address %s\n", uspi->arg);
      if (comma) *comma = ',';
      free(sn);
      return;
    }
    sn->addr_len = socket_to_in_addr(uspi->arg, colon, 0, &sn->addr.sa_in);

    sn->fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (sn->fd < 0) {
      uwsgi_error("stats_pusher_dd()/socket()");
      if (comma) *comma = ',';
                        free(sn);
                        return;
    }
    uwsgi_socket_nb(sn->fd);
    if (comma) *comma = ',';
    uspi->data = sn;
    uspi->configured = 1;
  }

  // we use the same buffer for all of the packets
  struct uwsgi_buffer *ub = uwsgi_buffer_new(uwsgi.page_size);
  struct uwsgi_metric *um = uwsgi.metrics;
  while(um) {
    uwsgi_rlock(uwsgi.metrics_lock);
    // ignore return value
    if (um->type == UWSGI_METRIC_GAUGE) {
      dd_send_metric(ub, uspi, um->name, um->name_len, *um->value, "|g");
    }
    else {
      dd_send_metric(ub, uspi, um->name, um->name_len, *um->value, "|c");
    }
    uwsgi_rwunlock(uwsgi.metrics_lock);
    if (um->reset_after_push){
      uwsgi_wlock(uwsgi.metrics_lock);
      *um->value = um->initial_value;
      uwsgi_rwunlock(uwsgi.metrics_lock);
    }
    um = um->next;
  }
  uwsgi_buffer_destroy(ub);
}

static void stats_pusher_dd_init(void) {
        struct uwsgi_stats_pusher *usp = uwsgi_register_stats_pusher("dd", stats_pusher_dd);
  // we use a custom format not the JSON one
  usp->raw = 1;
}

struct uwsgi_plugin stats_pusher_dd_plugin = {

        .name = "stats_pusher_dd",
        .on_load = stats_pusher_dd_init,
};

